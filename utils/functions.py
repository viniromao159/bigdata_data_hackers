"""
Funções compartilhadas entre as camadas Bronze, Silver e Gold.

Consolidado a partir do que cada notebook (bronze_2023_2024, silver_2023_2024,
gold_2023_2024) vinha redefinindo localmente. Import sugerido:

    from utils.functions import (
        extrair_codigo_e_descricao,
        col_segura,
        slug,
        obter_bloco,
        padronizar_bloco,
    )
"""

import re
import unicodedata

from pyspark.sql import DataFrame, functions as F


# --------------------------------------------------------------------------
# Bronze: resolver o nome bruto da coluna (formato de tupla Python em texto)
# --------------------------------------------------------------------------

PADRAO_TUPLA = re.compile(r"^\('([^']*)',\s*'(.*)\)$", re.DOTALL)


def extrair_codigo_e_descricao(nome_coluna_bruto):
    """Separa código da pergunta e descrição a partir do nome bruto de coluna,
    que vem no formato "('P1_a ', 'Idade')" na exportação original do CSV.

    Retorna (codigo, descricao). Se não casar com o padrão esperado,
    retorna (None, nome_coluna_bruto).
    """
    m = PADRAO_TUPLA.match(nome_coluna_bruto)
    if not m:
        return None, nome_coluna_bruto
    codigo, descricao = m.group(1).strip(), m.group(2)
    if descricao.endswith("'"):
        descricao = descricao[:-1]
    return codigo, descricao


# Edição 2025: o cabeçalho vem como "codigo_descricao" (ou "codigo descricao"),
# com código numérico pontuado (ex: "1.a_idade", "8.d.11_Criando e mantendo...").
# Formato diferente do de tupla de 2023, por isso um parser próprio. O separador
# entre código e descrição é inconsistente na exportação de 2025: a maioria usa
# "_", mas algumas colunas de opção de bloco usam espaço.
PADRAO_2025 = re.compile(r"^(\d+\.[a-z](?:\.\d+)?)[_ ](.*)$", re.DOTALL)


def extrair_codigo_e_descricao_2025(nome_coluna_bruto):
    """Separa (codigo, descricao) do nome bruto no formato 'codigo_descricao'
    ou 'codigo descricao' usado na edição 2025.

    Retorna (codigo, descricao). Se não casar com o padrão, retorna
    (None, nome_coluna_bruto).
    """
    m = PADRAO_2025.match(nome_coluna_bruto)
    if not m:
        return None, nome_coluna_bruto
    return m.group(1).strip(), m.group(2).strip()


# --------------------------------------------------------------------------
# Silver/Gold: referenciar colunas com segurança e gerar nomes de variável
# --------------------------------------------------------------------------

def col_segura(nome):
    """F.col() que funciona mesmo com nomes de coluna contendo caracteres
    especiais (ponto, espaço, crase) — ex: '.NET'."""
    return F.col(f"`{nome.replace(chr(96), chr(96) + chr(96))}`")


def slug(texto):
    """Nome de variável simples a partir de um texto: sem acento, minúsculo,
    com underscore. Remove também sufixo de desambiguação (__codigo), se houver."""
    texto = re.sub(r"__[A-Za-z0-9_.]+$", "", texto)
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    texto = re.sub(r"[^a-zA-Z0-9]+", "_", texto).strip("_").lower()
    return texto


# --------------------------------------------------------------------------
# Silver: blocos de múltipla escolha (bloco "guarda-chuva" -> colunas de opção)
# --------------------------------------------------------------------------

def obter_bloco(codigo_cabecalho, mapa_codigo_para_nome):
    """Colunas de opção de um bloco de múltipla escolha, dado o código
    "guarda-chuva" (ex: 'P4_d' -> todas as linguagens) e o de-para
    código -> nome de coluna carregado da Bronze (mapa_colunas_<ano>.csv)."""
    return [
        nome
        for codigo, nome in mapa_codigo_para_nome.items()
        if codigo.startswith(codigo_cabecalho) and codigo != codigo_cabecalho
    ]


def padronizar_bloco(df, nome_bloco, codigo_bloco, mapa_codigo_para_nome, condicao_escopo=None):
    """Converte as colunas de um bloco de múltipla escolha (que chegam como
    texto '1'/'0') para boolean de verdade, respeitando o escopo da pergunta.

    condicao_escopo: Column booleana (ex: F.col('aplica_analise_tecnica')) ou
    None se o bloco não tiver restrição de escopo (ex: 'experiencia_prejudicada').
    Quando informada, quem está fora do escopo recebe None em vez de True/False
    — assim, um simples `SUM(CASE WHEN coluna THEN 1 ELSE 0 END)` na Gold já
    exclui automaticamente quem está fora do escopo, sem precisar repetir o
    WHERE em toda query.

    Ver a tabela "Regras de negócio" no notebook da Silver para o mapeamento
    de qual bloco usa qual condição de escopo (aplica_analise_tecnica,
    aplica_analise_gestor, satisfeito_empresa == False, ou nenhum).
    """
    for coluna in obter_bloco(codigo_bloco, mapa_codigo_para_nome):
        valor = col_segura(coluna).cast("int")
        resultado = F.when(valor == 1, True).when(valor == 0, False).otherwise(None)
        if condicao_escopo is not None:
            resultado = F.when(condicao_escopo == False, None).otherwise(resultado)  # noqa: E712
        df = df.withColumn(coluna, resultado)
    return df


# --------------------------------------------------------------------------
# Edição 2024: funções próprias do pipeline do Maycon
# --------------------------------------------------------------------------

def ler_csv_bruto(spark, path: str) -> DataFrame:
    """Lê o CSV bruto da pesquisa State of Data.

    multiLine + escape/quote são obrigatórios: respostas de texto livre
    da pesquisa têm quebra de linha e vírgula dentro do próprio campo,
    e sem essas opções o Spark desalinha colunas silenciosamente.
    """
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(path)
    )


def renomear_e_filtrar_colunas(df: DataFrame, rename_colunas: dict) -> DataFrame:
    """Seleciona só as colunas relevantes (presentes no dicionário) e
    renomeia para o nome tratado. Ignora colunas do dicionário que não
    existirem no DataFrame (evita quebrar se o schema mudar)."""
    colunas_existentes = set(df.columns)
    selects = [
        F.col(f"`{original}`").alias(tratada)
        for original, tratada in rename_colunas.items()
        if original in colunas_existentes
    ]
    return df.select(*selects)


def colunas_multipla_escolha_para_boolean(df: DataFrame, colunas: list) -> DataFrame:
    """Converte colunas de múltipla escolha (0.0/1.0/NaN) para boolean.

    NaN é preservado como null -- significa que a pergunta nunca apareceu
    pra essa pessoa (lógica de bloco condicional), não que o dado "sumiu".
    """
    for c in colunas:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("boolean"))
    return df


def adicionar_coluna_edicao(df: DataFrame, edicao: str) -> DataFrame:
    """Adiciona a coluna `edicao` (ex: '2024_2025'), necessária pra
    depois dar UNION nas 3 silvers sem perder de qual pesquisa veio
    cada linha."""
    return df.withColumn("edicao", F.lit(edicao))


def relatorio_nulos(df: DataFrame) -> DataFrame:
    """Retorna um DataFrame com a contagem e o percentual de nulos por
    coluna -- útil pra conferir rapidamente se o percentual de nulo de
    uma coluna bate com o esperado pela regra de nulo documentada."""
    total = df.count()
    exprs = [
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]
    contagem = df.select(*exprs).first().asDict()
    linhas = [
        (coluna, qtd, round(100.0 * qtd / total, 1))
        for coluna, qtd in contagem.items()
    ]
    return df.sparkSession.createDataFrame(
        linhas, ["coluna", "qtd_nulos", "pct_nulos"]
    ).orderBy(F.desc("pct_nulos"))
