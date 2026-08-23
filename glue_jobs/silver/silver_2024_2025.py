#!/usr/bin/env python
# coding: utf-8

# # State of Data Brasil 2024 — Silver
# ### Tech Challenge Fase 3 — Grupo 6
# ### Edição 2024
#
# Dono: Maycon
#
# A Silver aplica as **regras de negócio** sobre a Bronze: seleciona as colunas
# que respondem às 7 perguntas do desafio, padroniza os nomes, trata os nulos
# estruturais com flags de escopo e converte os blocos de múltipla escolha pra
# boolean.
#
# **Princípio de nomes:** os apelidos seguem o **vocabulário canônico
# compartilhado entre as 3 edições** (`faixa_salarial`, `genero`,
# `banco_dados__mysql`, ...) — é isso que permite o Athena ler as partições das
# 3 edições como uma tabela só.
#
# **Regras de negócio abaixo, verificadas nesta base (não copiadas de outra
# edição):**
# - Grupo sem vínculo empregatício confirmado cruzando os nulos de
#   `cargo_atual`/`faixa_salarial` com `situacao_trabalho` — mesmas 4 categorias
#   da edição 2025, mas checado direto nesta base.
# - Bifurcação gestor × técnico confirmada (`atua_como_gestor=true` → nunca tem
#   `cargo_atual`/`nivel_senioridade` preenchido, e vice-versa) — mesmo padrão
#   de 2024/2025.
# - **Achado específico desta edição:** as colunas booleanas (`atua_como_gestor`,
#   `satisfeito_atualmente`) vêm como texto `"TRUE"`/`"FALSE"`, não `"1"`/`"0"`
#   como em 2023 e 2025 — confirmado direto no CSV bruto antes de escrever a
#   comparação, pra não repetir o bug de comparação que já apareceu 2x no grupo.
# - Nenhuma categoria de faixa (salário, porte de empresa) fora do padrão nesta
#   edição — conferido, sem achado (diferente de 2025, que teve 2 typos).
#
# **Versão de teste no Glue:** `utils/config.py` e `utils/functions.py` não
# estão disponíveis no cluster, então os caminhos e as funções auxiliares foram
# colados direto no script (mesmo padrão já validado no script de Silver 2025).

# In[ ]:


from pyspark.sql import SparkSession, functions as F
import re
from functools import partial

BUCKET = "s3://state-of-data-2023-1819-2244-3791"
CAMINHO_BRONZE = f"{BUCKET}/data/bronze/state_of_data"
CAMINHO_BRONZE_METADADOS = f"{BUCKET}/data/bronze/metadados"
CAMINHO_SILVER = f"{BUCKET}/data/silver/state_of_data_silver"
CAMINHO_SILVER_METADADOS = f"{BUCKET}/data/silver/metadados"

ANO_PESQUISA = 2024

# Funções mecânicas compartilhadas entre as 3 edições (coladas aqui porque
# utils/functions.py não está disponível no cluster -- mesmas 4 funções do
# script de Silver 2025).

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
    """Converte cada opção do bloco pra boolean. Aceita tanto '1'/'0' quanto
    'TRUE'/'FALSE' como texto -- achado desta edição (ver nota no topo)."""
    for coluna in _obter_bloco(codigo_bloco, mapa_codigo_para_nome):
        valor = col_segura(coluna)
        resultado = (
            F.when(valor.isin("1", "TRUE", "True", "true"), True)
             .when(valor.isin("0", "FALSE", "False", "false"), False)
             .otherwise(None)
        )
        if condicao_escopo is not None:
            resultado = F.when(condicao_escopo == False, None).otherwise(resultado)  # noqa: E712
        df = df.withColumn(coluna, resultado)
    return df

spark = SparkSession.builder.appName(f"state-of-data-{ANO_PESQUISA}-silver").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# ## 1. Carregando a Bronze (só a partição desta edição)
#
# `mergeSchema=true` é obrigatório aqui: as 3 edições têm schemas Bronze
# diferentes. Sem essa opção, o Spark lê o schema de uma única partição
# (normalmente a alfabeticamente primeira, "ano_pesquisa=2023") e aplica ele
# em cima de todas -- inclusive esta, cujas colunas que não existem em 2023
# sumiriam ou virariam nulo. Mesma causa raiz corrigida no script de Silver
# 2025.

# In[ ]:


df = (
    spark.read.option("mergeSchema", "true").parquet(str(CAMINHO_BRONZE))
    .filter(F.col("ano_pesquisa") == ANO_PESQUISA)
)
print(f"{df.count()} linhas, {len(df.columns)} colunas")


# ## 2. Carregando o mapa de colunas gerado na Bronze

# In[ ]:


CAMINHO_MAPA_COLUNAS = f"{CAMINHO_BRONZE_METADADOS}/mapa_colunas_2024.csv"

# FIX: nomes de coluna com aspas internas (ex: '"Point and Click" Analytics')
# foram escapados no padrão RFC4180 (aspas duplicadas) quando o CSV de mapa foi
# escrito. O Spark usa escape="\\" por padrão, não aspas -- preciso dizer
# explicitamente pra ele usar aspas como escape, igual já fazemos na leitura
# do CSV bruto da pesquisa (mesmo fix aplicado no script de Silver 2025).
df_mapa = (
    spark.read
    .option("header", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(str(CAMINHO_MAPA_COLUNAS))
)
mapa_codigo_para_nome = {row["codigo"]: row["nome_coluna"] for row in df_mapa.collect()}

obter_bloco = partial(_obter_bloco, mapa_codigo_para_nome=mapa_codigo_para_nome)

def nome(codigo):
    """Nome da coluna (pós-Bronze) a partir do código da pergunta."""
    return mapa_codigo_para_nome[codigo]

print(f"{len(mapa_codigo_para_nome)} códigos mapeados")

# Checagem de sanidade: todo nome que o mapa promete tem que existir no schema
# da Bronze lida agora (comparação case-insensitive, já que o Spark também
# ignora maiúscula/minúscula ao resolver nomes de coluna -- mesmo fix aplicado
# no script de Silver 2025).
colunas_df_lower = {c.lower() for c in df.columns}
nomes_esperados_ausentes = [
    (codigo, nome_coluna) for codigo, nome_coluna in mapa_codigo_para_nome.items()
    if nome_coluna.lower() not in colunas_df_lower
]
if nomes_esperados_ausentes:
    raise ValueError(
        f"{len(nomes_esperados_ausentes)} nome(s) do mapa de colunas não existem no "
        f"schema da Bronze lida agora. Exemplos: {nomes_esperados_ausentes[:5]}"
    )
print("OK: todos os nomes do mapa de colunas existem no schema da Bronze lida.")


# ## 3. Duplicidade de registros (por `token`)
#
# CONFIRMADO com dado real da base 2024: a coluna "id" está inteiramente nula
# (`df.groupBy("id").count()` só retornou NULL). O identificador de resposta de
# verdade é o código "0.a" (descrito como "token" no cabeçalho bruto) -- mesmo
# padrão encontrado na edição 2025. "0.d" (data/hora de envio) é só o carimbo de
# quando a resposta foi enviada, não serve pra dedup.

# In[ ]:


col_id = nome("0.a")
antes = df.count()
df = df.dropDuplicates([col_id])
depois = df.count()
print(f"Linhas antes: {antes} | depois de remover duplicatas: {depois} | removidas: {antes - depois}")


# ## 4. Mapeando as colunas pelas 7 perguntas do desafio
#
# Só os apelidos que a Gold de fato consulta (mesmo conjunto usado nas edições
# 2023 e 2025) — códigos verificados direto contra o cabeçalho desta base.

# In[ ]:


MAPA_SIMPLES = {
    "situacao_trabalho": "2.a",
    "setor_empresa": "2.b",
    "cargo_atual": "2.f",
    "nivel_senioridade": "2.g",
    "faixa_salarial": "2.h",
    "atua_como_gestor": "2.d",
    "num_funcionarios": "2.c",
    "tempo_experiencia_dados": "2.i",
    "tempo_experiencia_ti": "2.j",
    "objetivo_carreira": "5.a",
    "genero": "1.b",
    "cor_raca_etnia": "1.c",
    "pcd": "1.d",
    "cloud_preferida": "4.i",
    "ia_prioridade": "3.e",
    "regiao_atual": "1.i.2",
    "modelo_trabalho_atual": "2.r",
    "modelo_trabalho_ideal": "2.s",
    "satisfeito_empresa": "2.k",
    "nivel_ensino": "1.l",
    "area_formacao": "1.m",
    "atitude_retorno_presencial": "2.t",
    "pretende_mudar_emprego": "2.n",
}

MAPA_BLOCOS = {
    "cargos_no_time_dados": "3.b",
    "experiencia_prejudicada": "1.e",
    "linguagens_trabalho": "4.d",   # bloco "dia a dia" -- ver nota abaixo
    "banco_dados": "4.g",
    "fontes_dados": "4.b",
    "cloud": "4.h",
    "ferramenta_bi": "4.j",
    "ia_tipo_uso": "4.l",           # escopo técnico -- ver nota abaixo
    "ia_motivos_nao_uso": "3.g",
    "ia_uso_pessoal": "4.m",
    "desafios_gestor": "3.d",
    "motivo_insatisfacao": "2.l",
}
print(f"{len(MAPA_SIMPLES)} colunas simples, {len(MAPA_BLOCOS)} blocos")


# **Nota sobre `linguagens_trabalho`:** nesta edição existem 2 perguntas de
# linguagem: `4.d` (múltipla escolha, "dia a dia") e `4.f` (resposta única,
# "preferida"). Uso `4.d` porque é o bloco de múltipla escolha de verdade — a
# edição 2025 usou o bloco "preferida" pra esse mesmo apelido, mas lá é esse
# que vem como múltipla escolha (a estrutura difere entre edições). Mantém o
# apelido canônico igual; a pergunta física por trás dele é a mais parecida
# disponível em cada base.
#
# **Nota sobre `ia_tipo_uso`:** igual à edição 2025, existem 2 blocos com as
# mesmas opções de uso de IA na empresa (`3.f` e `4.l`), com públicos diferentes.
# Uso `4.l` (seção 4, técnica) como o `ia_tipo_uso` canônico -- mesma lógica
# documentada pra 2025, aqui confirmada pela posição do bloco (seção 4 é a
# seção técnica, gated por `cargo_atual`).

# Validação: todo código usado tem que existir no mapa, e todo bloco tem que ter opções.

# In[ ]:


faltando_simples = [c for c in MAPA_SIMPLES.values() if c not in mapa_codigo_para_nome]
faltando_blocos = [alias for alias, cod in MAPA_BLOCOS.items() if not obter_bloco(cod)]
if faltando_simples or faltando_blocos:
    print("ATENÇÃO, não encontrado:", faltando_simples, faltando_blocos)
else:
    print(f"OK: {len(MAPA_SIMPLES)} códigos simples e {len(MAPA_BLOCOS)} blocos confirmados.")


# ## 5. Tratando os nulos estruturais — flags de escopo
#
# | Flag | Regra (verificada nesta base) |
# | :-- | :-- |
# | `aplica_analise_emprego` | `situacao_trabalho` fora do grupo sem vínculo (4 categorias — mesmo conjunto de 2025, confirmado por 0%/100% de preenchimento de `faixa_salarial`). |
# | `aplica_analise_tecnica` | `cargo_atual` preenchido. |
# | `aplica_analise_gestor` | `atua_como_gestor = "TRUE"` (texto, não `"1"` — ver achado no topo do script). |
# | `aplica_analise_busca_oportunidade` | inverso de `aplica_analise_emprego`. |

# In[ ]:


col_situacao = mapa_codigo_para_nome.get("2.a")
col_cargo = mapa_codigo_para_nome.get("2.f")
col_gestor = mapa_codigo_para_nome.get("2.d")
col_funcao = mapa_codigo_para_nome.get("4.a.1")

# Categorias sem vínculo empregatício ativo na base 2024
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
    .withColumn("aplica_analise_gestor",
                col_segura(col_gestor).isin("1", "TRUE", "True", "true"))
)
df = df.withColumn("aplica_analise_busca_oportunidade", ~F.col("aplica_analise_emprego"))

# Flags por função de atuação (só existem se a pergunta 4.a.1 estiver mapeada
# nesta base -- guarda a checagem pra não quebrar se o código não existir).
if col_funcao is not None:
    df = (
        df
        .withColumn("aplica_bloco_engenharia_dados", col_segura(col_funcao) == "Engenharia de Dados")
        .withColumn("aplica_bloco_analise_dados", col_segura(col_funcao) == "Análise de Dados")
        .withColumn("aplica_bloco_ciencia_dados", col_segura(col_funcao) == "Outra atuação")
    )

print("Flags de escopo para a edição 2024 criadas com sucesso.")


# ## 6. Padronizando os blocos de múltipla escolha para boolean
#
# Antes, converto `satisfeito_empresa` pra boolean (`"TRUE"`/`"FALSE"` como
# texto), porque o escopo de `motivo_insatisfacao` depende dele.

# In[ ]:


col_satisfeito = nome("2.k")
df = df.withColumn(
    col_satisfeito,
    F.when(col_segura(col_satisfeito).isin("1", "TRUE", "True", "true"), True)
     .when(col_segura(col_satisfeito).isin("0", "FALSE", "False", "false"), False)
     .otherwise(None).cast("boolean")
)

def escopo_do_bloco(alias):
    if alias == "motivo_insatisfacao":
        return col_segura(col_satisfeito) == False  # noqa: E712
    if alias in {"cargos_no_time_dados", "desafios_gestor"}:
        return F.col("aplica_analise_gestor")
    if alias in {"linguagens_trabalho", "banco_dados", "fontes_dados", "cloud",
                 "ferramenta_bi", "ia_tipo_uso"}:
        return F.col("aplica_analise_tecnica")
    return None  # experiencia_prejudicada, ia_motivos_nao_uso, ia_uso_pessoal: sem escopo

for alias, codigo in MAPA_BLOCOS.items():
    df = _padronizar_bloco(df, alias, codigo, mapa_codigo_para_nome, escopo_do_bloco(alias))

print("Blocos convertidos para boolean, respeitando o escopo de cada um.")


# ## 7. Montando e exportando a Silver final

# In[ ]:


selects = [col_segura(nome(codigo)).alias(alias) for alias, codigo in MAPA_SIMPLES.items()]
for alias, codigo in MAPA_BLOCOS.items():
    for c in obter_bloco(codigo):
        selects.append(col_segura(c).alias(f"{alias}__{slug(c)}"))
selects += [
    F.col("aplica_analise_emprego"), F.col("aplica_analise_tecnica"),
    F.col("aplica_analise_gestor"), F.col("aplica_analise_busca_oportunidade"),
    F.col("ano_pesquisa"),
]

df_silver = df.select(*selects)
print(f"Silver final: {len(df_silver.columns)} colunas")

# ATENÇÃO -- ponto pra confirmar com o grupo: a seção 5 cria
# 'aplica_bloco_engenharia_dados' / 'aplica_bloco_analise_dados' /
# 'aplica_bloco_ciencia_dados' quando 'col_funcao' existe na base, mas esses 3
# não estão na lista de 'selects' acima -- ou seja, mesmo criados, não saem no
# Parquet final da Silver. Isso é intencional (2024 não usa esse recorte na
# Gold) ou esqueceram de incluir? Não mudei sozinho porque não sei a intenção
# de negócio aqui -- se for pra incluir, é só adicionar
# F.col('aplica_bloco_engenharia_dados') etc. na lista de selects acima.

df_silver.write \
    .mode("overwrite") \
    .partitionBy("ano_pesquisa") \
    .parquet(str(CAMINHO_SILVER))

print(f"Silver exportada com sucesso em: {CAMINHO_SILVER}")
print(f"Linhas: {df_silver.count()} | Colunas: {len(df_silver.columns)}")


# ## 8. Resumo
#
# 1. **Vocabulário:** apelidos batendo com o conjunto que a Gold já consulta
#    nas 7 perguntas — testável direto contra o mesmo SQL.
# 2. **Nulos estruturais:** 4 flags de escopo, mesma lógica das outras edições.
# 3. **Achado específico:** boolean como texto `"TRUE"`/`"FALSE"`, não `"1"`/`"0"`
#    -- a conversão dos blocos e das flags aceita as duas formas.
# 4. **Categorias:** sem erro de digitação encontrado nesta edição.
#
# **Próximo passo:** rodar a Gold (mesmas 29 tabelas de 2023/2025, `ANO_PESQUISA = 2024`).
