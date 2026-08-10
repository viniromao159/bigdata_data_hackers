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

from pyspark.sql import functions as F


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
