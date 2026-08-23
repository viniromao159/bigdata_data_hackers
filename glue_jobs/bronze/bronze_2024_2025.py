#!/usr/bin/env python
# coding: utf-8

# # State of Data Brasil 2024 — Bronze
# ### Tech Challenge Fase 3 — Grupo 6
# ### Edição 2024
#
# Dono: Maycon
#
# Esta é a camada **Bronze** da edição 2024 — a porta de entrada do dado no Data
# Lake. Aqui **não se toma nenhuma decisão de negócio**: só se ingere o dado como
# ele veio, resolve o mínimo pra ele ser utilizável (o nome das colunas), registra
# metadado de ingestão e exporta em Parquet.
#
# **Formato do cabeçalho desta base:** segue o mesmo padrão `codigo_descricao`
# (ex: `2.a_situação_de_trabalho`) que a edição 2025 do Vini — diferente do
# formato de tupla Python em texto que a edição 2023 usa. Por isso reaproveito o
# mesmo parser (`extrair_codigo_e_descricao_2025`) em vez de escrever um novo.
#
# **Versão de teste no Glue:** `utils/functions.py` e `utils/config.py` não
# estão disponíveis no cluster (não existe checkout do repo aqui), então as
# funções e os caminhos foram colados direto no script (mesma lógica dos
# módulos do repositório -- e o mesmo padrão adotado no script de Bronze 2025
# que já validamos rodando no Glue).
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

ANO_PESQUISA = 2024

# Parser do cabeçalho desta base (mesma lógica de utils/functions.py, reaproveitado
# do notebook de Bronze 2025): código numérico pontuado com 1 ou 2 níveis
# (ex: "1.a", "8.d.11"), seguido de "_" OU espaço, e o resto é a descrição.
PADRAO_2025 = re.compile(r"^(\d+\.[a-z](?:\.\d+)?)[_ ](.*)$", re.DOTALL)

def extrair_codigo_e_descricao_2025(nome_coluna_bruto):
    m = PADRAO_2025.match(nome_coluna_bruto)
    if not m:
        return None, nome_coluna_bruto
    return m.group(1).strip(), m.group(2).strip()

spark = SparkSession.builder.appName(f"state-of-data-{ANO_PESQUISA}-bronze").getOrCreate()

# Bronze é um caminho ÚNICO particionado por ano_pesquisa, compartilhado pelas 3
# edições. O overwrite padrão do Spark ("static") apagaria a pasta inteira
# (inclusive as partições dos outros anos). Com "dynamic", o overwrite só afeta
# a partição que este script realmente escreve (ano_pesquisa=2024).
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

print(f"Ano de pesquisa: {ANO_PESQUISA}")
print("Raw:", CAMINHO_RAW)
print("Bronze:", CAMINHO_BRONZE)


# A pesquisa tem perguntas abertas (texto livre) que podem conter vírgula e quebra
# de linha dentro da resposta. Sem `multiLine` + `quote`/`escape`, o Spark quebraria
# as colunas no lugar errado.

# In[ ]:


CAMINHO_CSV_ORIGEM = f"{CAMINHO_RAW}/state_of_data_2024_2025.csv"

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
# Os nomes vêm no formato `codigo_descricao`, com separador inconsistente
# (`_` na maioria, espaço em algumas colunas de opção de bloco) -- o parser
# aceita os dois.

# In[ ]:


pares_teste = [extrair_codigo_e_descricao_2025(c) for c in df.columns]
sem_codigo = [nome for codigo, nome in pares_teste if codigo is None]
print(f"Colunas sem código identificado: {len(sem_codigo)} (esperado: 0)")


# Desambiguação: alguns blocos têm opções com a MESMA descrição de texto, só o
# código muda. Sem isso, o rename colide (2 colunas -> 1 nome). Quando a
# descrição se repete, adiciono o código como sufixo (`descricao__codigo`).

# In[ ]:


pares = [extrair_codigo_e_descricao_2025(c) for c in df.columns]
contagem = Counter(descricao for _, descricao in pares)

novos_nomes, mapa_codigo_para_nome = [], {}
for codigo, descricao in pares:
    nome_final = f"{descricao}__{codigo}" if (contagem[descricao] > 1 and codigo) else descricao
    novos_nomes.append(nome_final)
    if codigo:
        mapa_codigo_para_nome[codigo] = nome_final

assert len(novos_nomes) == len(set(novos_nomes)), "Ainda há nomes duplicados!"
df = df.toDF(*novos_nomes)
print("Renomeação concluída, sem duplicatas.")
print(f"{len(mapa_codigo_para_nome)} códigos mapeados")


# **Importante pra Silver:** pra montar os blocos de múltipla escolha depois, a
# Silver precisa saber qual código virou qual nome de coluna. Salvo o
# `mapa_codigo_para_nome` como uma tabelinha ao lado do Parquet.
#
# Escrevo com Python puro (não via Spark) e subo direto pro S3 via `boto3` --
# não dá pra confiar em disco local persistente no cluster do Glue (mesmo
# ajuste já feito no script de Bronze 2025).

# In[ ]:


import csv as csv_module
import io
import boto3

CAMINHO_MAPA_COLUNAS = f"{CAMINHO_BRONZE_METADADOS}/mapa_colunas_2024.csv"

buffer = io.StringIO()
escritor = csv_module.writer(buffer)
escritor.writerow(["codigo", "nome_coluna"])
for codigo, nome in mapa_codigo_para_nome.items():
    escritor.writerow([codigo, nome])

bucket, chave = CAMINHO_MAPA_COLUNAS.replace("s3://", "").split("/", 1)
boto3.client("s3").put_object(Bucket=bucket, Key=chave, Body=buffer.getvalue().encode("utf-8"))

print(f"Mapa de colunas salvo em: {CAMINHO_MAPA_COLUNAS} ({len(mapa_codigo_para_nome)} códigos)")


# ## 3. Auditoria rápida (só registrar, sem corrigir nada aqui)
#
# A Bronze não corrige qualidade — mas vale registrar o que se encontra, pra
# conferir depois que a Silver tratou tudo.
#
# **Achado desta edição (confirmado direto no dado):** a coluna literal `id`
# vem inteiramente nula em 2024 -- não serve como identificador de resposta.
# O identificador de verdade é o código `0.a` ("token"). A Bronze só registra
# isso aqui; quem decide o que fazer com duplicidade é a Silver.

# In[ ]:


col_token = mapa_codigo_para_nome.get("0.a")
total = df.count()
distintos_token = df.select(col_token).distinct().count() if col_token else None

print(f"Total de linhas: {total}")
if col_token:
    print(f"Valores distintos de '0.a' (token): {distintos_token}")
    print("Sem duplicidade" if total == distintos_token else "ATENÇÃO: existem tokens duplicados (tratamento fica pra Silver)")
else:
    print("ATENÇÃO: código '0.a' não encontrado no mapa desta edição.")


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
    .withColumn("ano_pesquisa", F.lit(ANO_PESQUISA))
)

print(f"Colunas finais (com metadados): {len(df_bronze.columns)}")
df_bronze.select("dt_ingestao", "arquivo_origem", "fonte_pesquisa", "ano_pesquisa").show(3, truncate=False)


# ## 5. Exportando para Parquet
#
# Particionado por `ano_pesquisa`. Com `partitionOverwriteMode = dynamic`
# (setado lá em cima), escrever a partição 2024 afeta só essa partição — o
# caminho é compartilhado por várias edições e não pode ser apagado por
# inteiro a cada escrita.

# In[ ]:


df_bronze.write \
    .mode("overwrite") \
    .partitionBy("ano_pesquisa") \
    .parquet(str(CAMINHO_BRONZE))

print(f"Bronze exportada com sucesso em: {CAMINHO_BRONZE}")
print(f"Linhas: {df_bronze.count()} | Colunas: {len(df_bronze.columns)}")


# ## 6. Conclusão
#
# A Bronze 2024 ficou com as colunas originais (renomeadas só pra virarem
# utilizáveis, sem decisão de negócio) + 4 colunas de metadado de ingestão,
# particionada por `ano_pesquisa=2024`. O mapa código → nome de coluna
# (`mapa_colunas_2024.csv`) ficou salvo ao lado, pra Silver reconstruir os
# blocos de múltipla escolha sem refazer esse trabalho.
