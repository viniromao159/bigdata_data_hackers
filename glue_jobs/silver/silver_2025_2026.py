#!/usr/bin/env python
# coding: utf-8

# # State of Data Brasil 2025 — Silver
# ### Tech Challenge Fase 3 — Grupo 6
# ### Edição 2025
# 
# Fonte: [Kaggle — State of Data Brasil](https://www.kaggle.com/datahackers/datasets)
# 
# A Silver aplica as **regras de negócio** sobre a Bronze: seleciona as colunas que
# respondem às 7 perguntas do desafio, padroniza os nomes de coluna, trata os nulos
# estruturais com flags de escopo e converte os blocos de múltipla escolha para
# boolean.
# 
# **Princípio de nomes:** os apelidos de saída seguem um **vocabulário canônico
# compartilhado entre as edições** da pesquisa (`faixa_salarial`, `genero`,
# `banco_dados__mysql`, ...), pra o Athena ler as partições de todos os anos como
# uma tabela só. As perguntas que existem só nesta edição (rotinas por papel
# DE/DA/DS, critérios de escolha de emprego, data lake/warehouse, etc.) entram com
# nomes próprios.
# 
# **As regras de negócio abaixo foram investigadas nesta base**, não presumidas:
# - A lista de situações "sem vínculo empregatício" foi derivada cruzando os nulos
#   com `situacao_trabalho`.
# - `funcao_de_atuacao` (Engenharia / Análise / "Outra atuação") é o que libera os
#   blocos por papel (seções 6/7/8).
# - Há 1 token duplicado, removido logo no início.
# 
# ## 1. Carregando a Bronze

# In[ ]:


from pyspark.sql import SparkSession, functions as F
import re
from functools import partial

BUCKET = "s3://state-of-data-2023-1819-2244-3791"
CAMINHO_BRONZE = f"{BUCKET}/data/bronze/state_of_data"
CAMINHO_BRONZE_METADADOS = f"{BUCKET}/data/bronze/metadados"
CAMINHO_SILVER = f"{BUCKET}/data/silver/state_of_data_silver"
CAMINHO_SILVER_METADADOS = f"{BUCKET}/data/silver/metadados"

# Versão de teste no Glue: utils/functions.py não está disponível no cluster,
# então colei as 4 funções mecânicas direto aqui por enquanto (mesma lógica
# do utils/functions.py do repositório -- compartilhadas entre as 3 edições).

def col_segura(nome):
    """F.col() que funciona mesmo com nomes de coluna contendo caractere
    especial (ponto, espaço, crase) -- ex: '.NET'."""
    return F.col(f"`{nome.replace(chr(96), chr(96)+chr(96))}`")


def slug(texto):
    import unicodedata
    texto = re.sub(r"__[A-Za-z0-9_.]+$", "", texto)
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    texto = re.sub(r"[^a-zA-Z0-9]+", "_", texto).strip("_").lower()
    return texto


def _obter_bloco(codigo_cabecalho, mapa_codigo_para_nome):
    return [nome for codigo, nome in mapa_codigo_para_nome.items()
            if codigo.startswith(codigo_cabecalho) and codigo != codigo_cabecalho]


def _padronizar_bloco(df, nome_bloco, codigo_bloco, mapa_codigo_para_nome, condicao_escopo=None):
    for coluna in _obter_bloco(codigo_bloco, mapa_codigo_para_nome):
        valor = col_segura(coluna).cast("int")
        resultado = F.when(valor == 1, True).when(valor == 0, False).otherwise(None)
        if condicao_escopo is not None:
            resultado = F.when(condicao_escopo == False, None).otherwise(resultado)  # noqa: E712
        df = df.withColumn(coluna, resultado)
    return df

spark = SparkSession.builder.appName("state-of-data-2025-silver").getOrCreate()
# Caminho compartilhado por várias edições: "dynamic" faz o overwrite afetar só
# a partição 2025, sem apagar as demais.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# Leio a Bronze e filtro **só a minha edição** — o caminho é compartilhado pelas 3
# (particionado por `ano_pesquisa`), então sem o filtro eu processaria os anos
# misturados assim que os colegas escreverem as partições deles.
#
# `mergeSchema=true` é obrigatório aqui: 2023 e 2025 têm schemas Bronze
# diferentes (399 x 388 colunas originais — colunas diferentes, não só linhas
# diferentes). Sem essa opção, o Spark lê o schema de uma única partição
# (normalmente a que vem primeiro em ordem alfabética, `ano_pesquisa=2023`) e
# aplica ele em cima de todas as partições — inclusive a 2025, cujas colunas
# que não existem em 2023 sumiriam ou virariam nulo. Com `mergeSchema`, o
# Spark une os schemas de todas as partições antes de ler.

# In[ ]:


df = spark.read.option("mergeSchema", "true").parquet(str(CAMINHO_BRONZE))
df = df.filter(F.col("ano_pesquisa") == 2025)
print(f"{df.count()} linhas, {len(df.columns)} colunas")


# ## 2. Carregando o mapa de colunas gerado na Bronze
# 
# A Bronze salvou o de-para `código → nome de coluna`. Uso ele por duas razões:
# (1) montar os blocos de múltipla escolha a partir do código "guarda-chuva"
# (`4.d` → todos os bancos), e (2) **referenciar todas as colunas por código em vez
# de digitar a descrição na mão**. Como os nomes desta base têm acento, `/`, `?`
# etc. (`ai_generativa_e_llm_é_uma_prioridade?`), mapear por código evita erro de
# transcrição.

# In[ ]:


CAMINHO_MAPA_COLUNAS = f"{CAMINHO_BRONZE_METADADOS}/mapa_colunas_2025.csv"
# quote/escape: um nome de coluna de 2025 tem vírgula E aspas dentro
# ("Point and Click" Analytics como Alteryx, Knime...). Sem essas opções o
# Spark quebraria o nome na vírgula ao reler o CSV salvo pela Bronze.
df_mapa = (spark.read.option("header", "true")
           .option("quote", '"').option("escape", '"')
           .csv(str(CAMINHO_MAPA_COLUNAS)))
mapa_codigo_para_nome = {row["codigo"]: row["nome_coluna"] for row in df_mapa.collect()}

# Helpers amarrados ao mapa desta edição
obter_bloco = partial(_obter_bloco, mapa_codigo_para_nome=mapa_codigo_para_nome)
def nome(codigo):
    """Nome da coluna (pós-Bronze) a partir do código da pergunta."""
    return mapa_codigo_para_nome[codigo]

print(f"{len(mapa_codigo_para_nome)} códigos mapeados")

# Checagem de sanidade: todo nome que o mapa promete tem que existir de fato no
# schema da Bronze que acabei de ler. Se o mapa_colunas_2025.csv (gerado numa
# execução da Bronze) e o Parquet lido agora não vierem da mesma execução --
# por exemplo, rodou a Bronze de novo e a Silver pegou uma versão antiga de um
# dos dois -- isso diverge, e prefiro travar aqui com uma mensagem clara do que
# deixar estourar um AnalysisException obscuro lá na frente.
#
# Comparação case-insensitive de propósito: o Spark resolve nomes de coluna sem
# diferenciar maiúscula/minúscula por padrão (spark.sql.caseSensitive=false), e
# com mergeSchema=true (necessário pois 2023 e 2025 têm schemas Bronze
# diferentes), colunas básicas que existem nas duas edições com casing
# diferente (ex: "idade" em 2025 x "Idade" em 2023) são unificadas pelo Spark
# numa só grafia -- df.columns mostra só uma delas, mesmo a coluna "existindo"
# de fato para qualquer código que a referencie via col_segura/F.col. Comparar
# de forma sensível a caixa geraria falso positivo aqui.
colunas_df_lower = {c.lower(): c for c in df.columns}
nomes_esperados_ausentes = [
    (codigo, nome_coluna) for codigo, nome_coluna in mapa_codigo_para_nome.items()
    if nome_coluna.lower() not in colunas_df_lower
]
if nomes_esperados_ausentes:
    raise ValueError(
        f"{len(nomes_esperados_ausentes)} nome(s) do mapa de colunas não existem no "
        f"schema da Bronze lida agora (nem ignorando maiúscula/minúscula) -- "
        f"mapa_colunas_2025.csv e o Parquet provavelmente vêm de execuções diferentes "
        f"da Bronze. Rode a Bronze de novo (do zero) e só depois a Silver. "
        f"Exemplos: {nomes_esperados_ausentes[:5]}"
    )
print("OK: todos os nomes do mapa de colunas existem no schema da Bronze lida.")


# ## 3. Duplicidade de registros
# 
# Cada linha deve ser uma pessoa. A Bronze sinalizou 1 token duplicado — aqui é o
# lugar de tratar (a Bronze só registra, a Silver decide). Removo a duplicata
# mantendo uma ocorrência.

# In[ ]:


# O identificador único de resposta é o campo de código "0.a" (descrito como
# "token" no cabeçalho bruto) -- NÃO a coluna literal "id" (que, testado,
# não é única por pessoa nesta base; provavelmente é uma flag/versão, não um
# identificador de respondente).
col_id = nome("0.a")
antes = df.count()
df = df.dropDuplicates([col_id])
depois = df.count()
print(f"Linhas antes: {antes} | depois de remover duplicatas: {depois} | removidas: {antes - depois}")


# ## 4. Mapeando as colunas pelas 7 perguntas do desafio
# 
# Dois dicionários, ambos indexados por **código** (resolvidos pra nome via o mapa):
# 
# - `MAPA_SIMPLES` — perguntas de resposta única (a pessoa marca uma opção).
# - `MAPA_BLOCOS` — blocos de múltipla escolha (código "guarda-chuva" → várias
#   colunas de opção).
# 
# Os apelidos (as chaves) seguem o vocabulário canônico compartilhado entre as
# edições onde o conceito existe; o resto são perguntas que só aparecem nesta
# edição.

# In[ ]:


# --- Resposta única: alias -> código da pergunta ---
MAPA_SIMPLES = {
    # ==== apelidos canônicos (compartilhados entre as edições) ====
    "situacao_trabalho": "2.a",
    "setor_empresa": "2.b",
    "cargo_atual": "2.f",
    "nivel_senioridade": "2.g",
    "faixa_salarial": "2.h",
    "tempo_experiencia_dados": "2.i",
    "tempo_experiencia_ti": "2.j",
    "genero": "1.b",
    "cor_raca_etnia": "1.c",
    "pcd": "1.d",
    "regiao_atual": "1.i.2",            # regiao_onde_mora
    "modelo_trabalho_atual": "2.q",
    "modelo_trabalho_ideal": "2.r",
    "satisfeito_empresa": "2.k",
    "ia_prioridade": "3.e",
    "pretende_mudar_emprego": "2.n",
    "atua_como_gestor": "2.d",
    "nivel_ensino": "1.l",
    "area_formacao": "1.m",
    "atitude_retorno_presencial": "2.s",
    "cloud_preferida": "4.f",
    "num_funcionarios": "2.c",
    "objetivo_carreira": "5.a",
    # ==== perguntas exclusivas desta edição ====
    "idade": "1.a",
    "faixa_idade": "1.a.1",
    "vive_no_brasil": "1.g",
    "pais_onde_mora": "1.h",
    "uf_onde_mora": "1.i.1",
    "vive_no_estado_formacao": "1.j",
    "uf_de_origem": "1.k.1",
    "regiao_de_origem": "1.k.2",
    "cargo_como_gestor": "2.e",
    "participou_entrevistas_6m": "2.m",
    "empresa_layoff_2025": "2.p",
    "num_pessoas_em_dados": "3.a",
    "empresa_resultados_llm": "3.g",
    "funcao_de_atuacao": "4.a.1",
    "ferramenta_bi_preferida": "4.h",
    "oportunidade_buscada": "5.b",
    "tempo_em_busca_oportunidade": "5.c",
    "experiencia_processos_seletivos": "5.d",
    "possui_data_lake": "6.c",
    "tecnologia_data_lake": "6.d",
    "possui_data_warehouse": "6.e",
    "tecnologia_data_warehouse": "6.f",
    "ferramentas_qualidade_dados": "6.g",
}

# --- Blocos de múltipla escolha: alias -> código "guarda-chuva" ---
MAPA_BLOCOS = {
    # ==== apelidos canônicos (compartilhados entre as edições) ====
    "experiencia_prejudicada": "1.e",
    "motivo_insatisfacao": "2.l",
    "cargos_no_time_dados": "3.b",
    "desafios_gestor": "3.d",
    "ia_motivos_nao_uso": "3.h",
    "fontes_dados": "4.b",
    "linguagens_trabalho": "4.c",       # bloco "linguagem preferida" (ver nota)
    "banco_dados": "4.d",
    "cloud": "4.e",
    "ferramenta_bi": "4.g",
    "ia_tipo_uso": "4.i",               # versão de escopo técnico (ver nota)
    "ia_uso_pessoal": "4.j",
    # ==== perguntas exclusivas desta edição ====
    "aspectos_prejudicados": "1.f",
    "criterios_escolha_emprego": "2.o",
    "responsabilidades_gestor": "3.c",
    "ia_tipo_uso_empresa": "3.f",       # público diferente de 4.i (ver nota)
    "rotina_de": "6.a",
    "ferramentas_etl_de": "6.b",
    "maior_tempo_de": "6.h",
    "rotina_da": "7.a",
    "ferramentas_etl_da": "7.b",
    "ferramentas_autonomia_negocios": "7.c",
    "maior_tempo_da": "7.d",
    "rotina_ds": "8.a",
    "tecnicas_metodos_ds": "8.b",
    "tecnologias_ds": "8.c",
    "maior_tempo_ds": "8.d",
}
print(f"{len(MAPA_SIMPLES)} colunas simples, {len(MAPA_BLOCOS)} blocos")


# Validação: todo código digitado tem que existir no mapa (evita typo de código)
# e todo bloco tem que ter opções.

# In[ ]:


faltando_simples = [c for c in MAPA_SIMPLES.values() if c not in mapa_codigo_para_nome]
faltando_blocos = [alias for alias, cod in MAPA_BLOCOS.items() if not obter_bloco(cod)]
if faltando_simples or faltando_blocos:
    print("ATENÇÃO, não encontrado:", faltando_simples, faltando_blocos)
else:
    print(f"OK: {len(MAPA_SIMPLES)} códigos simples e {len(MAPA_BLOCOS)} blocos confirmados.")


# ## 5. Tratando os nulos estruturais — flags de escopo
# 
# Investiguei os nulos desta base (cruzando cada bloco com a coluna que o "libera")
# e encontrei os grupos de pergunta condicional de 2025. Em vez de rótulo de texto
# "Não se aplica", crio **colunas booleanas de escopo** — assim qualquer filtro em
# SQL vira só `WHERE aplica_analise_emprego = true`.
# 
# | Flag | Regra (verificada nesta base) |
# | :-- | :-- |
# | `aplica_analise_emprego` | `situacao_trabalho` fora do grupo sem vínculo (267 pessoas: desempregado buscando, 2 tipos de estudante, acadêmico). |
# | `aplica_analise_tecnica` | `cargo_atual` preenchido (libera linguagens/banco/cloud/BI/IA técnica). |
# | `aplica_analise_gestor` | `atua_como_gestor = 1` (libera cargos no time, responsabilidades, desafios). |
# | `aplica_analise_busca_oportunidade` | inverso de `aplica_analise_emprego`. |
# | `aplica_bloco_engenharia_dados` | `funcao_de_atuacao = Engenharia de Dados` (libera seção 6 / DE). |
# | `aplica_bloco_analise_dados` | `funcao_de_atuacao = Análise de Dados` (libera seção 7 / DA). |
# | `aplica_bloco_ciencia_dados` | `funcao_de_atuacao = Outra atuação` (libera seção 8 / DS — em 2025 a seção de Ciência de Dados foi respondida por esse grupo). |

# In[ ]:


col_situacao = nome("2.a")
col_cargo = nome("2.f")
col_gestor = nome("2.d")
col_funcao = nome("4.a.1")

# Grupo sem vínculo empregatício ativo (re-derivado das categorias de 2025 que
# compartilham exatamente os 267 nulos das colunas de empresa).
NAO_APLICA_EMPREGO = [
    "Desempregado, buscando recolocação",
    "Somente Estudante (graduação)",
    "Somente Estudante (pós-graduação)",
    "Trabalho na área Acadêmica/Pesquisador",
]

df = (
    df
    .withColumn("aplica_analise_emprego", ~col_segura(col_situacao).isin(*NAO_APLICA_EMPREGO))
    .withColumn("aplica_analise_tecnica", col_segura(col_cargo).isNotNull())
    .withColumn("aplica_analise_gestor", col_segura(col_gestor) == "1")
    .withColumn("aplica_bloco_engenharia_dados", col_segura(col_funcao) == "Engenharia de Dados")
    .withColumn("aplica_bloco_analise_dados", col_segura(col_funcao) == "Análise de Dados")
    .withColumn("aplica_bloco_ciencia_dados", col_segura(col_funcao) == "Outra atuação")
)
df = df.withColumn("aplica_analise_busca_oportunidade", ~F.col("aplica_analise_emprego"))
print("Flags de escopo criadas.")


# ## 6. Corrigindo categorias fora do padrão
# 
# Ao revisar as perguntas de faixa (salário e porte da empresa), encontrei duas
# categorias que fogem do padrão das demais. Cada uma pede uma decisão diferente,
# pela natureza do erro:
# 
# - **Faixa salarial:** `"de R$ 25.001/mês a R$ 3000/mês"` (1 pessoa) — é um typo
#   óbvio do "30.000", e existe uma única leitura plausível. Corrijo pra
#   `"de R$ 25.001/mês a R$ 30.000/mês"` em vez de descartar, pra preservar uma
#   resposta válida.
# - **Porte da empresa:** `"de 501 a 100"` (1 pessoa) — junta o início de uma faixa
#   ("501 a 1.000") com o fim de outra ("51 a 100"), e são portes bem diferentes.
#   Como não dá pra saber qual a pessoa quis dizer, marco como nulo: corrigir errado
#   inventaria um dado, o que é pior que deixar vazio.

# In[ ]:


col_salario = nome("2.h")
col_porte = nome("2.c")

df = df.withColumn(
    col_salario,
    F.when(col_segura(col_salario) == "de R$ 25.001/mês a R$ 3000/mês",
           "de R$ 25.001/mês a R$ 30.000/mês")
     .otherwise(col_segura(col_salario))
)
df = df.withColumn(
    col_porte,
    F.when(col_segura(col_porte) == "de 501 a 100", None).otherwise(col_segura(col_porte))
)
print("Categorias fora do padrão tratadas (salário corrigido, porte ambíguo -> nulo).")


# ## 7. Padronizando os blocos de múltipla escolha para boolean
# 
# Os blocos vêm como texto `'1'`/`'0'`/vazio — precisam virar boolean de verdade
# (um `isNotNull()` ingênuo trataria o `'0'` como "marcou"). A conversão respeita o
# escopo de cada bloco: quem está fora do escopo recebe `None` (não `False`), pra o
# `SUM(CASE WHEN coluna THEN 1 ELSE 0 END)` da Gold já excluir automaticamente.
# 
# Antes, converto `satisfeito_empresa` pra boolean, porque o escopo de
# `motivo_insatisfacao` depende dele.
# 
# **Notas de mapeamento (decisões desta edição):**
# - `linguagens_trabalho` ← bloco `4.c`, que nesta base é a "linguagem
#   **preferida**". Mantive o apelido canônico pra a análise de linguagens da Gold
#   ficar unificada entre as edições.
# - `ia_tipo_uso` ← `4.i`. Existem dois blocos com as mesmas opções de uso de IA
#   (`3.f` e `4.i`), mas com públicos diferentes: `4.i` é respondido por quem tem
#   cargo técnico (escopo técnico) e `3.f` por quem não tem. Uso `4.i` como o
#   `ia_tipo_uso` canônico e trago o `3.f` como `ia_tipo_uso_empresa`, sem escopo,
#   pra não descartar essas respostas.
# - `ia_motivos_nao_uso` ← `3.h`. A investigação mostrou que quem responde esse
#   bloco é o grupo sem cargo técnico; deixei sem filtro de escopo pra não zerar
#   respostas válidas.

# In[ ]:


# satisfeito_empresa -> boolean
col_satisfeito = nome("2.k")
df = df.withColumn(
    col_satisfeito,
    F.when(col_segura(col_satisfeito) == "1", True)
     .when(col_segura(col_satisfeito) == "0", False)
     .otherwise(None).cast("boolean")
)

# Escopo de cada bloco (fruto da investigação de nulo desta base)
ESCOPO_GESTOR = {"cargos_no_time_dados", "responsabilidades_gestor", "desafios_gestor"}
ESCOPO_TECNICA = {"fontes_dados", "linguagens_trabalho", "banco_dados", "cloud",
                  "ferramenta_bi", "ia_tipo_uso", "ia_uso_pessoal"}
ESCOPO_EMPREGO = {"criterios_escolha_emprego"}
ESCOPO_DE = {"rotina_de", "ferramentas_etl_de", "maior_tempo_de"}
ESCOPO_DA = {"rotina_da", "ferramentas_etl_da", "ferramentas_autonomia_negocios", "maior_tempo_da"}
ESCOPO_DS = {"rotina_ds", "tecnicas_metodos_ds", "tecnologias_ds", "maior_tempo_ds"}
# Sem escopo (perguntas de perfil ou de público próprio): experiencia_prejudicada,
# aspectos_prejudicados, ia_tipo_uso_empresa, ia_motivos_nao_uso

def escopo_do_bloco(alias):
    if alias == "motivo_insatisfacao":
        return col_segura(col_satisfeito) == False  # noqa: E712
    if alias in ESCOPO_GESTOR:
        return F.col("aplica_analise_gestor")
    if alias in ESCOPO_TECNICA:
        return F.col("aplica_analise_tecnica")
    if alias in ESCOPO_EMPREGO:
        return F.col("aplica_analise_emprego")
    if alias in ESCOPO_DE:
        return F.col("aplica_bloco_engenharia_dados")
    if alias in ESCOPO_DA:
        return F.col("aplica_bloco_analise_dados")
    if alias in ESCOPO_DS:
        return F.col("aplica_bloco_ciencia_dados")
    return None

for alias, codigo in MAPA_BLOCOS.items():
    df = _padronizar_bloco(df, alias, codigo, mapa_codigo_para_nome, escopo_do_bloco(alias))

print("Blocos convertidos para boolean, respeitando o escopo de cada um.")


# ## 8. Gerando o dicionário de colunas reutilizável (`dict_columns_2025.py`)
# 
# Gero um arquivo Python com o de-para completo `nome_original → apelido`, agrupado
# pelas 7 perguntas do desafio. Assim os notebooks de análise (Gold) só dão
# `import dict_columns_2025` em vez de redigitar esse mapeamento — o que evita que
# cópias desatualizadas circulem e gerem inconsistência.

# In[ ]:


PERGUNTAS_CONCEITOS = {
    "p1_estrutura_mercado": [
        "situacao_trabalho", "setor_empresa", "cargo_atual", "nivel_senioridade",
        "faixa_salarial", "atua_como_gestor", "cargos_no_time_dados", "num_funcionarios",
        "funcao_de_atuacao", "num_pessoas_em_dados", "cargo_como_gestor",
        "responsabilidades_gestor", "empresa_layoff_2025",
    ],
    "p2_perfis_valorizados": [
        "cargo_atual", "nivel_senioridade", "faixa_salarial",
        "tempo_experiencia_dados", "tempo_experiencia_ti", "objetivo_carreira",
        "idade", "faixa_idade", "criterios_escolha_emprego", "participou_entrevistas_6m",
        "oportunidade_buscada", "tempo_em_busca_oportunidade", "experiencia_processos_seletivos",
    ],
    "p3_diversidade": [
        "genero", "cor_raca_etnia", "pcd", "cargo_atual", "nivel_senioridade",
        "faixa_salarial", "experiencia_prejudicada", "aspectos_prejudicados",
    ],
    "p4_tecnologias": [
        "linguagens_trabalho", "banco_dados", "fontes_dados", "cloud", "ferramenta_bi",
        "cloud_preferida", "ferramenta_bi_preferida",
        "rotina_de", "ferramentas_etl_de", "maior_tempo_de",
        "rotina_da", "ferramentas_etl_da", "ferramentas_autonomia_negocios", "maior_tempo_da",
        "rotina_ds", "tecnicas_metodos_ds", "tecnologias_ds", "maior_tempo_ds",
        "possui_data_lake", "tecnologia_data_lake", "possui_data_warehouse",
        "tecnologia_data_warehouse", "ferramentas_qualidade_dados",
    ],
    "p5_ia_generativa": [
        "ia_prioridade", "ia_tipo_uso", "ia_motivos_nao_uso", "ia_uso_pessoal",
        "ia_tipo_uso_empresa", "empresa_resultados_llm",
    ],
    "p6_diferencas_regionais": [
        "regiao_atual", "nivel_senioridade", "faixa_salarial", "modelo_trabalho_atual",
        "modelo_trabalho_ideal", "satisfeito_empresa", "nivel_ensino", "area_formacao",
        "atitude_retorno_presencial", "uf_onde_mora", "pais_onde_mora", "vive_no_brasil",
        "vive_no_estado_formacao", "uf_de_origem", "regiao_de_origem",
    ],
    "p7_oportunidades_desafios": [
        "desafios_gestor", "ia_motivos_nao_uso", "motivo_insatisfacao", "pretende_mudar_emprego",
    ],
}

def conceitos_para_dict(lista_conceitos):
    resultado = {"id": "token"}
    for conceito in lista_conceitos:
        if conceito in MAPA_SIMPLES:
            resultado[nome(MAPA_SIMPLES[conceito])] = conceito
        elif conceito in MAPA_BLOCOS:
            for opcao in obter_bloco(MAPA_BLOCOS[conceito]):
                resultado[opcao] = f"{conceito}__{slug(opcao)}"
    return resultado

linhas = ['"""Dicionário de colunas -- edição 2025, agrupado por pergunta.',
          'Apelidos no vocabulário canônico compartilhado entre as edições; perguntas',
          'exclusivas desta edição entram com nome próprio.',
          '"""', '']
for nome_pergunta, conceitos in PERGUNTAS_CONCEITOS.items():
    d = conceitos_para_dict(conceitos)
    linhas.append(f"{nome_pergunta} = {{")
    for k, v in d.items():
        linhas.append(f"    {k!r}: {v!r},")
    linhas.append("}")
    linhas.append("")

linhas.append("rename_colunas = {}")
linhas.append("for d in [" + ", ".join(PERGUNTAS_CONCEITOS.keys()) + "]:")
linhas.append("    rename_colunas.update(d)")
linhas.append("")
linhas.append("colunas_por_pergunta = {")
for nome_pergunta in PERGUNTAS_CONCEITOS:
    linhas.append(f"    {nome_pergunta!r}: list({nome_pergunta}.values()),")
linhas.append("}")

import io
import boto3

CAMINHO_DICT_COLUNAS = f"{CAMINHO_SILVER_METADADOS}/dict_columns_2025.py"
conteudo = "\n".join(linhas)

bucket, chave = CAMINHO_DICT_COLUNAS.replace("s3://", "").split("/", 1)
boto3.client("s3").put_object(Bucket=bucket, Key=chave, Body=conteudo.encode("utf-8"))

print(f"Arquivo {CAMINHO_DICT_COLUNAS} gerado com sucesso.")


# ## 9. Montando e exportando a Silver final
# 
# Seleciono as colunas simples (com o apelido) + as opções de bloco (nome
# `bloco__opcao`) + as flags de escopo + `ano_pesquisa`, e
# exporto em Parquet particionado — mesmo caminho compartilhado, `dynamic`
# overwrite grava só a partição 2025.

# In[ ]:


selects = [col_segura(nome(codigo)).alias(alias) for alias, codigo in MAPA_SIMPLES.items()]
for alias, codigo in MAPA_BLOCOS.items():
    for c in obter_bloco(codigo):
        selects.append(col_segura(c).alias(f"{alias}__{slug(c)}"))
selects += [
    F.col("aplica_analise_emprego"), F.col("aplica_analise_tecnica"),
    F.col("aplica_analise_gestor"), F.col("aplica_analise_busca_oportunidade"),
    F.col("aplica_bloco_engenharia_dados"), F.col("aplica_bloco_analise_dados"),
    F.col("aplica_bloco_ciencia_dados"), F.col("ano_pesquisa"),
]

df_silver = df.select(*selects)
print(f"Silver final: {len(df_silver.columns)} colunas")


# In[ ]:


df_silver.write \
    .mode("overwrite") \
    .partitionBy("ano_pesquisa") \
    .parquet(str(CAMINHO_SILVER))

print(f"Silver exportada com sucesso em: {CAMINHO_SILVER}")
print(f"Linhas: {df_silver.count()} | Colunas: {len(df_silver.columns)}")


# ## 10. Resumo
# 
# 1. **Duplicidade:** 1 token duplicado removido.
# 2. **Nomes:** apelidos no vocabulário canônico compartilhado entre as edições
#    (pro Athena ler as partições como uma tabela só) + perguntas exclusivas desta
#    edição com nome próprio.
# 3. **Nulos estruturais:** 7 flags de escopo derivadas desta base — incluindo a
#    dimensão por `funcao_de_atuacao` (DE/DA/DS).
# 4. **Categorias fora do padrão:** salário com typo corrigido; porte ambíguo -> nulo.
# 5. **Blocos:** convertidos pra boolean respeitando o escopo certo de cada um.
# 6. **Dicionário:** `dict_columns_2025.py` gerado, agrupado pelas 7 perguntas.
# 
# **Próximos passos:** rodar no AWS Academy Lab, catalogar no Glue e conferir os
# apelidos batendo entre as edições no Athena.
