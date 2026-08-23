#!/usr/bin/env python
# coding: utf-8

# # State of Data Brasil 2025 — Bronze
# ### Tech Challenge Fase 3 — Grupo 6
# ### Edição 2025
# 
# Fonte: [Kaggle — State of Data Brasil](https://www.kaggle.com/datahackers/datasets)
# 
# Esta é a camada **Bronze** da edição 2025 — a porta de entrada do dado no Data
# Lake. Aqui **não se toma nenhuma decisão de negócio**: só se ingere o dado como
# ele veio, resolve o mínimo pra ele ser utilizável (o nome das colunas), registra
# metadado de ingestão e exporta em Parquet.
# 
# **Formato do cabeçalho desta base:** os nomes de coluna vêm como
# `codigo_descricao` numa string só, com código numérico pontuado (`1.a`,
# `8.d.11`). Pra o dado ser referenciável no Spark eu preciso separar código e
# descrição — é o único "conserto" que a Bronze faz, e ele é técnico, não de
# negócio.
# 
# **Versão de teste no Glue:** `utils/functions.py` não está disponível no
# cluster (não existe checkout do repo aqui), então o parser
# (`extrair_codigo_e_descricao_2025`) foi colado direto no notebook por
# enquanto (mesma lógica do `utils/functions.py` do repositório).
# 
# Todas as 388 colunas originais continuam aqui, sem seleção — quem decide o que
# interessa é a Silver.
# 
# ## 1. Preparando o ambiente e carregando a base

# In[ ]:


from pyspark.sql import SparkSession, functions as F
import re
from collections import Counter

BUCKET = "s3://state-of-data-2023-1819-2244-3791"
CAMINHO_RAW = f"{BUCKET}/data/raw"
CAMINHO_BRONZE = f"{BUCKET}/data/bronze/state_of_data"
CAMINHO_BRONZE_METADADOS = f"{BUCKET}/data/bronze/metadados"

# Parser do cabeçalho desta base (mesma lógica de utils/functions.py):
# código numérico pontuado com 1 ou 2 níveis (ex: "1.a", "8.d.11"), seguido de
# "_" OU espaço, e o resto é a descrição. O separador é inconsistente na
# exportação de 2025 -- a maioria usa "_", mas ~30 colunas de opção de bloco
# usam espaço.
PADRAO_2025 = re.compile(r"^(\d+\.[a-z](?:\.\d+)?)[_ ](.*)$", re.DOTALL)

def extrair_codigo_e_descricao_2025(nome_coluna_bruto):
    m = PADRAO_2025.match(nome_coluna_bruto)
    if not m:
        return None, nome_coluna_bruto
    return m.group(1).strip(), m.group(2).strip()

spark = SparkSession.builder.appName("state-of-data-2025-bronze").getOrCreate()

# Bronze é um caminho ÚNICO particionado por ano_pesquisa, escrito pelos 3
# integrantes. O overwrite padrão do Spark ("static") apagaria a pasta inteira
# (inclusive as partições dos outros anos). Com "dynamic", o overwrite só afeta
# a partição que este notebook realmente escreve (ano_pesquisa=2025).
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# A pesquisa tem perguntas abertas (texto livre) que podem conter vírgula e quebra
# de linha dentro da resposta. Sem `multiLine` + `quote`/`escape`, o Spark quebraria
# as colunas no lugar errado.

# In[ ]:


CAMINHO_CSV_ORIGEM = f"{CAMINHO_RAW}/state_of_data_2025_2026.csv"

df = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(str(CAMINHO_CSV_ORIGEM))
)

print(f"{df.count()} linhas, {len(df.columns)} colunas")


# ## 2. Resolvendo o nome das colunas
# 
# Os nomes vêm no formato `codigo_descricao` — ex: `1.a_idade`, `1.b_genero`,
# `8.d.11_Criando e mantendo a infra...`. O código tem 1 ou 2 níveis: o "pai" de
# uma pergunta (`8.d`) e as opções de um bloco de múltipla escolha (`8.d.1`,
# `8.d.2`, ...).
# 
# Uma pegadinha do CSV de 2025: o separador entre código e descrição é
# **inconsistente** — a maioria usa `_` (`1.a_idade`), mas ~30 colunas de opção de
# bloco usam **espaço** (`3.f.1 Colaboradores usando AI...`). O parser precisa
# aceitar os dois.

# In[ ]:


df.columns[:6]


# O parser (`extrair_codigo_e_descricao_2025`, definido acima) captura o código
# (`\d+.letra` com um nível opcional `.n`) seguido de `_` **ou** espaço, e o
# resto como descrição. Testo contra as 388 colunas antes de aplicar — o
# esperado é 0 falha.

# In[ ]:


pares_teste = [extrair_codigo_e_descricao_2025(c) for c in df.columns]
sem_codigo = [nome for codigo, nome in pares_teste if codigo is None]
print(f"Colunas sem código identificado: {len(sem_codigo)} (esperado: 0)")


# Cada opção de um bloco de múltipla escolha já tem uma descrição própria, então a
# maioria dos nomes de coluna é única. Ainda assim, **43 descrições se repetem**
# entre colunas diferentes (ex: "Databricks" aparece nas ferramentas de ETL de mais
# de um papel; "Remuneração/Salário" aparece em critério de escolha e em motivo de
# insatisfação). Quando a descrição se repete, adiciono o código como sufixo
# (`descricao__codigo`) pra garantir nome único e sem ambiguidade — do contrário o
# Spark não conseguiria distinguir as duas colunas.

# In[ ]:


pares = [extrair_codigo_e_descricao_2025(c) for c in df.columns]
contagem = Counter(descricao for _, descricao in pares)

# Só coloco o código junto do nome quando a descrição se repete em mais de uma
# coluna (blocos de múltipla escolha e opções com rótulo idêntico entre blocos).
novos_nomes, mapa_codigo_para_nome = [], {}
for codigo, descricao in pares:
    nome_final = f"{descricao}__{codigo}" if (contagem[descricao] > 1 and codigo) else descricao
    novos_nomes.append(nome_final)
    if codigo:
        mapa_codigo_para_nome[codigo] = nome_final

assert len(novos_nomes) == len(set(novos_nomes)), "Ainda há nomes duplicados!"
df = df.toDF(*novos_nomes)
print("Renomeação concluída, sem duplicatas.")
print("\nExemplo de nomes finais:")
for c in df.columns[:6]:
    print(" -", c)


# **Importante pra Silver:** pra montar os blocos de múltipla escolha depois
# (ex: "todas as ferramentas de ETL que a pessoa marcou"), a Silver precisa saber
# qual código (`4.d.1`, `4.d.2`...) virou qual nome de coluna. Vou salvar o
# `mapa_codigo_para_nome` como uma tabelinha ao lado do Parquet — assim a Silver
# só carrega o de-para pronto.
# 
# Escrevo com Python puro (não via Spark): a tabela é pequena (388 linhas) e criar
# DataFrame Spark a partir de lista Python pura costuma dar problema de worker no
# Spark local no Windows. Subo direto pro S3 via `boto3` (sem tocar em disco
# local, que não existe/persiste do jeito certo no cluster do Glue).

# In[ ]:


import csv as csv_module
import io
import boto3

CAMINHO_MAPA_COLUNAS = f"{CAMINHO_BRONZE_METADADOS}/mapa_colunas_2025.csv"

# monta o CSV em memória (sem tocar em disco local)
buffer = io.StringIO()
escritor = csv_module.writer(buffer)
escritor.writerow(["codigo", "nome_coluna"])
for codigo, nome in mapa_codigo_para_nome.items():
    escritor.writerow([codigo, nome])

# sobe direto pro S3 via boto3
bucket, chave = CAMINHO_MAPA_COLUNAS.replace("s3://", "").split("/", 1)
boto3.client("s3").put_object(Bucket=bucket, Key=chave, Body=buffer.getvalue().encode("utf-8"))

print(f"Mapa de colunas salvo em: {CAMINHO_MAPA_COLUNAS} ({len(mapa_codigo_para_nome)} códigos)")


# ## 3. Auditoria rápida (só registrar, sem corrigir nada aqui)
# 
# A Bronze não corrige qualidade — mas vale registrar o que se encontra, pra
# conferir depois que a Silver tratou tudo. Duas checagens: duplicidade de registro
# e volume de nulo por coluna (só contando).

# In[ ]:


col_id = df.columns[0]
total = df.count()
distintos = df.select(col_id).distinct().count()

print(f"Total de linhas: {total}")
print(f"IDs distintos: {distintos}")
print("Sem duplicidade" if total == distintos else "ATENÇÃO: existem IDs duplicados (tratamento fica pra Silver)")


# In[ ]:


# Contagem de nulo por coluna, só como registro de auditoria
total_linhas = df.count()
nulos_por_coluna = df.select([
    F.sum(F.col(f"`{c}`").isNull().cast("int")).alias(c) for c in df.columns
]).collect()[0].asDict()

colunas_com_mais_nulo = sorted(nulos_por_coluna.items(), key=lambda x: -x[1])[:10]
print("Top 10 colunas com mais nulo (informativo, não tratado aqui):")
for nome, qtd in colunas_com_mais_nulo:
    print(f"  {qtd:5d} ({qtd/total_linhas*100:5.1f}%)  {nome}")


# ## 4. Adicionando metadados de ingestão
# 
# Colunas de controle, padrão de qualquer Bronze de Data Lake: de onde veio cada
# linha, quando chegou, e o ano da pesquisa (que também é a chave de partição).

# In[ ]:


df_bronze = (
    df
    .withColumn("dt_ingestao", F.current_timestamp())
    .withColumn("arquivo_origem", F.input_file_name())
    .withColumn("fonte_pesquisa", F.lit("State of Data Brasil - Data Hackers + Bain"))
    .withColumn("ano_pesquisa", F.lit(2025))
)

print(f"Colunas finais (com metadados): {len(df_bronze.columns)}")
df_bronze.select("dt_ingestao", "arquivo_origem", "fonte_pesquisa", "ano_pesquisa").show(3, truncate=False)


# ## 5. Exportando para Parquet
# 
# Particionado por `ano_pesquisa`. Com `partitionOverwriteMode = dynamic` (setado
# lá em cima), escrever a partição 2025 afeta só essa partição — o caminho é
# compartilhado por várias edições e não pode ser apagado por inteiro a cada
# escrita.

# In[ ]:


df_bronze.write \
    .mode("overwrite") \
    .partitionBy("ano_pesquisa") \
    .parquet(str(CAMINHO_BRONZE))

print(f"Bronze exportada com sucesso em: {CAMINHO_BRONZE}")
print(f"Linhas: {df_bronze.count()} | Colunas: {len(df_bronze.columns)}")


# ## 6. Conclusão
# 
# A Bronze 2025 ficou com as 388 colunas originais (renomeadas só pra virarem
# utilizáveis, sem decisão de negócio) + 4 colunas de metadado de ingestão,
# particionada por `ano_pesquisa=2025`. O mapa código → nome de coluna
# (`mapa_colunas_2025.csv`) ficou salvo ao lado, pra Silver reconstruir os blocos
# de múltipla escolha sem refazer esse trabalho.
