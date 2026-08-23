#!/usr/bin/env python
# coding: utf-8

# # State of Data Brasil 2023 — Bronze
# ### Tech Challenge Fase 3 — Grupo 6
# ### Responsável pela edição 2023: Carlos Henrique Freitas
# 
# Fonte: [Kaggle — State of Data Brasil](https://www.kaggle.com/datahackers/datasets)
# 
# Esse notebook é a camada **Bronze**: a porta de entrada do dado no nosso Data
# Lake. Aqui eu **não tomo nenhuma decisão de negócio**. 
# 
# O que se fez aqui:
# 1. **Ingerir o dado bruto** exatamente como ele chegou da fonte (Data
#    Hackers/Kaggle).
# 2. Resolver só o que impede o dado de ser sequer **utilizável** (nesse caso, o
#    nome das colunas, que veio num formato que quebra referência de coluna no
#    Spark - não é uma "correção de negócio", é o mínimo pra conseguir ler o
#    dado depois).
# 3. Adicionar **metadados de ingestão** (de onde veio, quando foi carregado) -
#    isso é o que chamam de rastreabilidade/lineage, importante pra auditoria.
# 4. Exportar em **Parquet**, formato colunar, mais eficiente que CSV pra
#    qualquer camada que vier depois ler.
# 
# Todas as 399 colunas originais continuam aqui, sem seleção nenhuma - quem
# decide o que interessa é a Silver, lendo esse Parquet.
# 
# ## 1. Preparando o ambiente e carregando a base

# In[1]:

from pyspark.sql import SparkSession, functions as F
import re
from collections import Counter

BUCKET = "s3://state-of-data-2023-1819-2244-3791"
CAMINHO_RAW = f"{BUCKET}/data/raw"
CAMINHO_BRONZE = f"{BUCKET}/data/bronze/state_of_data"
CAMINHO_BRONZE_METADADOS = f"{BUCKET}/data/bronze/metadados"

# Versão de teste no Glue: utils/functions.py não está disponível no cluster
# (não existe checkout do repo aqui), então colei a função direto por enquanto.
PADRAO_TUPLA = re.compile(r"^\('([^']*)',\s*'(.*)\)$", re.DOTALL)

def extrair_codigo_e_descricao(nome_coluna_bruto):
    m = PADRAO_TUPLA.match(nome_coluna_bruto)
    if not m:
        return None, nome_coluna_bruto
    codigo, descricao = m.group(1).strip(), m.group(2)
    if descricao.endswith("'"):
        descricao = descricao[:-1]
    return codigo, descricao

spark = SparkSession.builder.appName("state-of-data-2023-bronze").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# In[4]:


# Fonte original: pesquisa State of Data Brasil, Data Hackers + Bain
CAMINHO_CSV_ORIGEM = f"{CAMINHO_RAW}/state_of_data_2023_2024.csv"

df = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(CAMINHO_CSV_ORIGEM)
)

print(f"{df.count()} linhas, {len(df.columns)} colunas")


# 5293 linhas, 399 colunas - nenhuma seleção ainda, é o dado inteiro como veio.
# 
# ## 2. Resolvendo o nome das colunas

# In[ ]:


df.columns[:5]


# **Achado:** os nomes de coluna vieram no formato de tupla Python em texto,
# tipo `"('P1_a ', 'Idade')"`. O cabeçalho original da pesquisa tem 2 níveis
# (código da pergunta + descrição), e na exportação pro CSV isso virou uma
# string só. Isso não é uma questão de "limpeza de negócio" - é uma questão
# técnica: sem resolver isso, nem dá pra referenciar essas colunas direito no
# Spark (teria que usar o nome bruto entre crases toda vez).
# 
# Escrevi uma função pra separar código e descrição, e testei contra as 399
# colunas antes de aplicar - encontrei 2 casos que uma regex mais simples não
# pega (uma descrição com apóstrofo no meio, tipo `"ETL's"`, e uma linha com
# aspa de fechamento faltando na exportação original do CSV). Ajustei a regex
# pra cobrir os dois casos. (a função extrair_codigo_e_descricao foi movida pra utils/functions.py, compartilhada com Maycon e Vini — aqui só fica o teste contra essa edição.)

# In[ ]:


pares_teste = [extrair_codigo_e_descricao(c) for c in df.columns]
sem_codigo = [desc for codigo, desc in pares_teste if codigo is None]
print(f"Colunas sem código identificado: {len(sem_codigo)} (esperado: 0)")


# In[ ]:


pares = [extrair_codigo_e_descricao(c) for c in df.columns]
contagem = Counter(descricao for _, descricao in pares)
# só coloco o código junto do nome quando a descrição se repete em mais de uma
# coluna (acontece nos blocos de múltipla escolha, onde cada opção de resposta
# tem uma pergunta "guarda-chuva" com a mesma descrição)

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


# **Importante pra quem for ler esse Parquet depois (Silver):** usei a mesma
# lógica de nome de coluna que uso lá - código da pergunta + descrição, com
# sufixo de código só quando o nome se repete entre colunas diferentes.
# 
# Só que percebi uma pegadinha nisso: pra montar os blocos de múltipla escolha
# depois (tipo "todas as opções de linguagem que a pessoa marcou"), a Silver
# precisa saber qual código de pergunta (`P4_d_1`, `P4_d_2`...) virou qual nome
# de coluna. Só que **312 das 399 colunas não levam esse código no nome final**
# (só levam sufixo as 87 que tinham descrição duplicada) - ou seja, se a Silver
# tentasse redescobrir isso só olhando pro nome da coluna depois de ler o
# Parquet, a informação já teria se perdido pra maioria dos blocos.
# 
# Pra não perder essa rastreabilidade, vou salvar esse `mapa_codigo_para_nome`
# também, como uma tabelinha separada ao lado do Parquet principal. Assim a
# Silver só precisa carregar esse de-para pronto, em vez de precisar reconstruir
# tudo de novo.
# 
# **Detalhe de implementação:** na primeira tentativa eu criei essa tabelinha
# via `spark.createDataFrame(lista_python, ...)` e deu erro
# (`Py4JJavaError: Timed out while waiting for the Python worker to connect
# back`). É um problema conhecido de rodar PySpark local no Windows - criar um
# DataFrame a partir de uma lista Python pura precisa de um vai-e-volta entre o
# driver e um worker Python, e esse worker às vezes não conecta a tempo. Como
# essa tabela é bem pequena (só 399 linhas, um código e um nome por linha), não
# faz sentido nem usar o Spark pra gerar ela - escrevo direto com Python puro,
# sem passar pelo Spark nessa etapa. O Spark só entra de novo depois, na hora de
# ler esse arquivo (isso sim é uma operação tranquila).

# In[ ]:


import csv as csv_module
import io
import boto3

CAMINHO_MAPA_COLUNAS = f"{CAMINHO_BRONZE_METADADOS}/mapa_colunas_2023.csv"

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


# **Nota pra quando isso for rodar no cluster de verdade (AWS Academy Lab):**
# `open()` só escreve em disco local - funciona bem aqui porque estou testando
# localmente, mas não escreve direto num bucket S3. Na hora de migrar, essa
# célula vira uma chamada de `boto3` pra subir esse arquivo pequeno pro S3 (ou,
# se preferir manter tudo em Spark, o `spark.createDataFrame(...).write.csv(...)`
# que tentei primeiro deve funcionar normalmente lá - o erro do worker
# foi uma particularidade do Spark local no Windows, não algo que eu espere ver
# no cluster do Glue/EMR).

# ## 3. Auditoria rápida (só pra registrar, sem corrigir nada aqui)
# 
# A Bronze não corrige problema de qualidade - mas vale **registrar** o que eu
# encontro, até pra confirmar depois que a Silver tratou tudo certinho. Duas
# checagens básicas: duplicidade de registro e volume de nulo por coluna (só
# contando, sem decidir o que fazer).

# In[ ]:


col_id = df.columns[0]
total = df.count()
distintos = df.select(col_id).distinct().count()

print(f"Total de linhas: {total}")
print(f"IDs distintos: {distintos}")
print("Sem duplicidade" if total == distintos else "ATENÇÃO: existem IDs duplicados (decisão de tratamento fica pra Silver)")


# In[ ]:


# Contagem de nulo por coluna, só como registro de auditoria - não vou decidir
# nada aqui sobre o que fazer com esses nulos, isso é investigação da Silver
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
# Antes de exportar, adiciono algumas colunas de controle - isso é padrão em
# qualquer camada Bronze de Data Lake, pra saber depois de onde veio cada linha
# e quando ela chegou (rastreabilidade/auditoria). Também aproveito pra marcar o
# ano da pesquisa - hoje só temos a edição 2023, mas o nome do arquivo original
# (`state_of_data_2023_2024.csv`) sugere que outras edições podem ser
# adicionadas aqui no futuro, então já deixo esse campo pronto.

# In[ ]:


df_bronze = (
    df
    .withColumn("dt_ingestao", F.current_timestamp())
    .withColumn("arquivo_origem", F.input_file_name())
    .withColumn("fonte_pesquisa", F.lit("State of Data Brasil - Data Hackers + Bain"))
    .withColumn("ano_pesquisa", F.lit(2023))
)

print(f"Colunas finais (com metadados): {len(df_bronze.columns)}")
df_bronze.select("dt_ingestao", "arquivo_origem", "fonte_pesquisa", "ano_pesquisa").show(3, truncate=False)


# ## 5. Exportando para Parquet
# 
# Escolhi particionar por `ano_pesquisa`
# isso já deixa o layout pronto pra meus colegas acrescentarem as outras edições da pesquisa
# (2024, 2025), sem precisar reestruturar nada.
# 
# O caminho abaixo é local pra eu testar - na submissão final do desafio, troca
# pelo caminho do bucket S3 do grupo, algo tipo
# `s3://<bucket-do-grupo>/bronze/state_of_data/`.

# In[ ]:


df_bronze.write \
    .mode("overwrite") \
    .partitionBy("ano_pesquisa") \
    .parquet(str(CAMINHO_BRONZE))

print(f"Bronze exportada com sucesso em: {CAMINHO_BRONZE}")
print(f"Linhas: {df_bronze.count()} | Colunas: {len(df_bronze.columns)}")


# ## 6. Conclusão
# 
# A Bronze ficou com as 399 colunas originais (renomeadas só pra virarem
# utilizáveis, sem nenhuma decisão de negócio), mais 4 colunas de metadado de
# ingestão, particionada por ano da pesquisa. Também deixei salvo, ao lado, o
# mapa de código → nome de coluna, pra ninguém precisar refazer esse trabalho de
# novo.
