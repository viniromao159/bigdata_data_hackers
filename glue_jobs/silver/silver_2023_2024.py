#!/usr/bin/env python
# coding: utf-8

# # State of Data Brasil 2023 — Silver
# ### Tech Challenge Fase 3 — Grupo 6
# ### Responsável pela edição 2023: Carlos Henrique Freitas
# 
# Fonte: [Kaggle — State of Data Brasil](https://www.kaggle.com/datahackers/datasets)
# 
# ## 1. Carregando a Bronze

# In[ ]:


from pyspark.sql import SparkSession, functions as F
import re

BUCKET = "s3://state-of-data-2023-1819-2244-3791"
CAMINHO_BRONZE = f"{BUCKET}/data/bronze/state_of_data"
CAMINHO_BRONZE_METADADOS = f"{BUCKET}/data/bronze/metadados"
CAMINHO_SILVER = f"{BUCKET}/data/silver/state_of_data_silver"
CAMINHO_SILVER_METADADOS = f"{BUCKET}/data/silver/metadados"

# Versão de teste no Glue: utils/functions.py não está disponível no cluster,
# então colei as 4 funções mecânicas direto aqui por enquanto (mesma lógica
# do utils/functions.py do repositório).

def col_segura(nome):
    """F.col() que funciona mesmo com nomes de coluna contendo ponto (ex: '.NET')."""
    return F.col(f"`{nome.replace(chr(96), chr(96)+chr(96))}`")


def slug(texto):
    import unicodedata
    texto = re.sub(r"__[A-Za-z0-9_.]+$", "", texto)
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    texto = re.sub(r"[^a-zA-Z0-9]+", "_", texto).strip("_").lower()
    return texto


def _obter_bloco_bruto(codigo_cabecalho, mapa_codigo_para_nome):
    return [nome for codigo, nome in mapa_codigo_para_nome.items()
            if codigo.startswith(codigo_cabecalho) and codigo != codigo_cabecalho]


def _padronizar_bloco(df, nome_bloco, codigo_bloco, mapa_codigo_para_nome, condicao_escopo=None):
    for coluna in _obter_bloco_bruto(codigo_bloco, mapa_codigo_para_nome):
        valor = col_segura(coluna).cast("int")
        resultado = F.when(valor == 1, True).when(valor == 0, False).otherwise(None)
        if condicao_escopo is not None:
            resultado = F.when(condicao_escopo == False, None).otherwise(resultado)  # noqa: E712
        df = df.withColumn(coluna, resultado)
    return df

spark = SparkSession.builder.appName("state-of-data-2023-silver").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# A Bronze já entrega o dado com nome de coluna tratado (resolveu o problema do
# nome vir em formato de tupla) e com metadado de ingestão. Então aqui eu só
# preciso ler o Parquet.

# In[ ]:


df = spark.read.parquet(CAMINHO_BRONZE)
df = df.filter(F.col("ano_pesquisa") == 2023)
print(f"{df.count()} linhas, {len(df.columns)} colunas")


# 5293 linhas e 403 colunas (as 399 originais + as 4 de metadado que a Bronze
# adicionou). Antes de seguir, preciso recuperar uma informação que vou usar
# bastante daqui pra frente: o de-para entre código da pergunta (`P4_d_1`,
# `P3_b_2`...) e o nome final da coluna. Isso é necessário porque, mais à
# frente, preciso montar os blocos de múltipla escolha (tipo "todas as opções
# de linguagem que a pessoa marcou") a partir do código da pergunta "guarda-
# chuva" - e a maioria dos nomes de coluna não carrega esse código no nome
# (só carregam sufixo as colunas cuja descrição se repetia em mais de um
# lugar). Por sorte, a Bronze já deixou esse mapeamento salvo numa tabela
# separada, então só preciso carregar.
# 
# ## 2. Carregando o mapa de colunas gerado na Bronze

# In[ ]:


CAMINHO_MAPA_COLUNAS = f"{CAMINHO_BRONZE_METADADOS}/mapa_colunas_2023.csv"

df_mapa = spark.read.option("header", "true").csv(CAMINHO_MAPA_COLUNAS)
mapa_codigo_para_nome = {row["codigo"]: row["nome_coluna"] for row in df_mapa.collect()}

from functools import partial
obter_bloco = partial(_obter_bloco_bruto, mapa_codigo_para_nome=mapa_codigo_para_nome)

print(f"{len(df.columns)} colunas carregadas da Bronze")
print(f"{len(mapa_codigo_para_nome)} códigos mapeados")


# ## 3. Conferindo duplicidade de registros
# 
# Antes de seguir, preciso confirmar que cada linha representa uma pessoa só -
# senão qualquer contagem que eu fizer depois já nasce errada.

# In[4]:


col_id = df.columns[0]
total = df.count()
distintos = df.select(col_id).distinct().count()

print(f"Total de linhas: {total}")
print(f"IDs distintos: {distintos}")
print("Sem duplicidade" if total == distintos else "ATENÇÃO: existem IDs duplicados")


# ## 4. Organizando as colunas pelas 7 perguntas do desafio
# 
# A base tem quase 400 colunas, mas boa parte é de sub-perguntas bem específicas.
# Pra não me perder, decidi organizar a seleção em função das **7 perguntas de
# negócio** que o Tech Challenge pede pra responder na apresentação executiva:
# estrutura do mercado, perfis mais valorizados, diversidade de gênero,
# tecnologias mais adotadas, adoção de IA, diferenças regionais/senioridade/
# modelo de trabalho, e oportunidades/desafios.
# 
# Separei em dois tipos de coluna:
# - **Resposta única** (`mapa_colunas`): perguntas de múltipla escolha onde a
#   pessoa só marca uma opção (ex: gênero, cargo atual).
# - **Blocos de múltipla escolha** (`mapa_blocos`): perguntas onde a pessoa pode
#   marcar mais de uma opção (ex: quais linguagens usa, quais bancos de dados
#   usa) - cada opção virou uma coluna separada na exportação, então preciso
#   buscar todas as colunas que pertencem ao mesmo "código guarda-chuva".

# **Nota de organização:** `col_segura` e `obter_bloco` foram movidas pra
# `utils/functions.py` — são puramente mecânicas (resolver nome de coluna com
# caractere especial, buscar as colunas de um bloco a partir do código
# "guarda-chuva"), não carregam nenhuma decisão de negócio, então não faz
# sentido cada edição reimplementar a sua.

# In[5]:


# Colunas de resposta única
mapa_colunas = {
    "situacao_trabalho": "Qual sua situação atual de trabalho?",
    "setor_empresa": "Setor",
    "cargo_atual": "Cargo Atual",
    "nivel_senioridade": "Nivel",
    "faixa_salarial": "Faixa salarial",
    "tempo_experiencia_dados": "Quanto tempo de experiência na área de dados você tem?",
    "tempo_experiencia_ti": "Quanto tempo de experiência na área de TI/Engenharia de Software "
                             "você teve antes de começar a trabalhar na área de dados?",
    "genero": "Genero",
    "cor_raca_etnia": "Cor/raca/etnia",
    "pcd": "PCD",
    "regiao_atual": "Regiao onde mora",
    "modelo_trabalho_atual": "Atualmente qual a sua forma de trabalho?",
    "modelo_trabalho_ideal": "Qual a forma de trabalho ideal para você?",
    "satisfeito_empresa": "Você está satisfeito na sua empresa atual?",
    "ia_prioridade": "AI Generativa é uma prioridade em sua empresa?",
    "pretende_mudar_emprego": "Você pretende mudar de emprego nos próximos 6 meses?",
    "atua_como_gestor": "Gestor?",
    "nivel_ensino": "Nivel de Ensino",
    "area_formacao": "Área de Formação",
    "atitude_retorno_presencial": "Caso sua empresa decida pelo modelo 100% presencial qual será sua atitude?",
    "cloud_preferida": "Dentre as opções listadas, qual sua Cloud preferida?",
    "num_funcionarios": "Numero de Funcionarios",
    "objetivo_carreira": "Qual seu objetivo na área de dados?",
}

# Blocos de múltipla escolha (código da pergunta "guarda-chuva")
mapa_blocos = {
    "linguagens_trabalho": "P4_d",
    "banco_dados": "P4_g",
    "fontes_dados": "P4_b",
    "cloud": "P4_h",
    "ferramenta_bi": "P4_j",
    "cargos_no_time_dados": "P3_b",
    "desafios_gestor": "P3_d",
    "motivo_insatisfacao": "P2_l",
    "ia_tipo_uso": "P4_l",
    "ia_motivos_nao_uso": "P3_g",
    "experiencia_prejudicada": "P1_e",
    "ia_uso_pessoal": "P4_m",
}


# Antes de confiar nesses dicionários, quero conferir se todo nome que eu digitei
# bate exatamente com o que existe na base - é muito fácil errar um acento ou um
# espaço na hora de copiar a descrição da pergunta.

# In[6]:


faltando_simples = [nome for nome in mapa_colunas.values() if nome not in df.columns]
faltando_blocos = [nome_bloco for nome_bloco, codigo in mapa_blocos.items() if not obter_bloco(codigo)]

if faltando_simples or faltando_blocos:
    print("Atenção, não encontrado:", faltando_simples, faltando_blocos)
else:
    print(f"Todas as colunas confirmadas: {len(mapa_colunas)} simples, {len(mapa_blocos)} blocos.")


# In[7]:


colunas_selecionadas = list(mapa_colunas.values())
for codigo in mapa_blocos.values():
    colunas_selecionadas += obter_bloco(codigo)

print(f"{len(colunas_selecionadas)} colunas selecionadas de {len(df.columns)} totais")
df.select([col_segura(c) for c in colunas_selecionadas]).show(5)


# ## 5. Gerando um dicionário de colunas reutilizável
# 
# Reparei que esse mapeamento (`mapa_colunas` + `mapa_blocos`) é informação
# valiosa que eu não quero perder nem duplicar. Se cada análise que eu ou
# alguém do grupo for fazer depois precisar copiar e colar esses dicionários de
# novo, mais cedo ou mais tarde alguém vai copiar uma versão desatualizada e
# gerar inconsistência.
# 
# Por isso decidi gerar um arquivo Python separado (`dict_columns_2023.py`) com
# o de-para completo: nome original da pergunta → nome final da coluna na
# Silver. Assim, qualquer notebook de análise só precisa dar `import
# dict_columns_2023` em vez de redigitar tudo.
# 
# Pra isso, primeiro preciso de uma função que transforme a descrição de cada
# pergunta num nome de coluna seguro (sem acento, minúsculo, com underscore no
# lugar de espaço/pontuação) - vou chamar de `slug`.

# **Nota de organização:** `slug` foi pra `utils/functions.py`, pelo mesmo
# motivo — é só normalização de texto (tirar acento, virar minúsculo,
# underscore), sem nada específico dessa edição da pesquisa.

# In[8]:


dict_columns_2023 = {}

# Colunas de resposta única
for alias, nome_original in mapa_colunas.items():
    dict_columns_2023[nome_original] = alias

# Blocos de múltipla escolha - uma entrada por opção real do bloco
for prefixo, codigo in mapa_blocos.items():
    for opcao in obter_bloco(codigo):
        dict_columns_2023[opcao] = f"{prefixo}__{slug(opcao)}"

print(f"{len(dict_columns_2023)} colunas mapeadas")


# Agora escrevo isso num arquivo `.py` de verdade, organizado pelas 7 perguntas
# do desafio (cada dicionário separado por pergunta, mais um dicionário geral
# `rename_colunas` juntando tudo) - assim fica fácil tanto importar tudo de uma
# vez quanto pegar só o pedaço de uma pergunta específica.

# In[ ]:


PERGUNTAS_CONCEITOS = {
    "p1_estrutura_mercado": [
        "situacao_trabalho", "setor_empresa", "cargo_atual", "nivel_senioridade",
        "faixa_salarial", "atua_como_gestor", "cargos_no_time_dados", "num_funcionarios"
    ],
    "p2_perfis_valorizados": [
        "cargo_atual", "nivel_senioridade", "faixa_salarial",
        "tempo_experiencia_dados", "tempo_experiencia_ti", "objetivo_carreira"
    ],
    "p3_diversidade": [
        "genero", "cor_raca_etnia", "pcd", "cargo_atual", "nivel_senioridade",
        "faixa_salarial", "experiencia_prejudicada",
    ],
    "p4_tecnologias": [
        "linguagens_trabalho", "banco_dados", "fontes_dados", "cloud", "ferramenta_bi",
        "cloud_preferida",
    ],
    "p5_ia_generativa": [
        "ia_prioridade", "ia_tipo_uso", "ia_motivos_nao_uso", "ia_uso_pessoal",
    ],
    "p6_diferencas_regionais": [
        "regiao_atual", "nivel_senioridade", "faixa_salarial", "modelo_trabalho_atual",
        "modelo_trabalho_ideal", "satisfeito_empresa", "nivel_ensino", "area_formacao",
        "atitude_retorno_presencial",
    ],
    "p7_oportunidades_desafios": [
        "desafios_gestor", "ia_motivos_nao_uso", "motivo_insatisfacao", "pretende_mudar_emprego",
    ],
}

def conceitos_para_dict(lista_conceitos):
    """A partir de uma lista de conceitos (aliases), monta {nome_original: alias}
    olhando tanto mapa_colunas quanto os blocos de mapa_blocos."""
    resultado = {"id": "token"}
    for conceito in lista_conceitos:
        if conceito in mapa_colunas:
            resultado[mapa_colunas[conceito]] = conceito
        elif conceito in mapa_blocos:
            for opcao in obter_bloco(mapa_blocos[conceito]):
                resultado[opcao] = f"{conceito}__{slug(opcao)}"
    return resultado

linhas = ['"""Dicionário de colunas -- edição 2023, agrupado por pergunta.',
          'Gerado a partir do mapeamento de colunas da Silver, já validado contra a base real.',
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

import os
import io
import boto3

CAMINHO_DICT_COLUNAS = f"{CAMINHO_SILVER_METADADOS}/dict_coluns_2023.py"
conteudo = "\n".join(linhas)

bucket, chave = CAMINHO_DICT_COLUNAS.replace("s3://", "").split("/", 1)
boto3.client("s3").put_object(Bucket=bucket, Key=chave, Body=conteudo.encode("utf-8"))

print(f"Arquivo {CAMINHO_DICT_COLUNAS} gerado com sucesso.")


# Com isso, da próxima vez que alguém do grupo (inclusive eu mesmo, num notebook
# diferente) precisar saber qual coluna original virou `faixa_salarial` ou quais
# colunas fazem parte do bloco `linguagens_trabalho`, é só importar esse arquivo
# em vez de reconstruir tudo de novo.

# ## 6. Investigando os nulos
# 
# Antes de decidir o que fazer com nulo, quero entender de onde ele vem. Base de
# pesquisa (survey) costuma ter pergunta condicional - tipo "só pergunta o cargo
# pra quem está empregado" - e isso gera nulo que não é erro, é nulo que "faz
# sentido existir". Vou contar os nulos de cada coluna de resposta única
# primeiro.

# In[10]:


df.select([
    F.sum(col_segura(c).isNull().cast("int")).alias(c) for c in mapa_colunas.values()
]).show(vertical=True, truncate=False)


# Reparei que várias colunas diferentes têm **exatamente 540 nulos**. Quando
# várias colunas têm o mesmo número exato de nulo, isso é sinal forte de
# pergunta condicional - o mesmo grupo de gente pulou aquele bloco inteiro. Vou
# testar essa hipótese cruzando com a situação de trabalho da pessoa.

# In[11]:


col_situacao = mapa_colunas["situacao_trabalho"]
col_salario = mapa_colunas["faixa_salarial"]

df.select(
    col_segura(col_situacao),
    col_segura(col_salario).isNull().alias("salario_nulo"),
).groupBy(col_situacao, "salario_nulo").count().orderBy(col_situacao).show(truncate=False)


# Bateu certinho: os nulos em `faixa_salarial` (e nas outras colunas do mesmo
# grupo de 540) são exatamente as pessoas que estão **desempregadas, são apenas
# estudantes, ou trabalham na área acadêmica** - ou seja, não têm uma "empresa
# atual" pra responder sobre setor, salário, cargo etc. Faz todo sentido a
# pesquisa ter pulado essa pergunta pra elas. **Não é erro de coleta.**
# 
# * Desempregados (em busca ou não de recolocação): 371 (361 + 10)
# * Estudantes (graduação e pós-graduação): 106 (78 + 28)
# * Área Acadêmica/Pesquisadores: 63
# * Total fora do mercado de trabalho ativo: 540 (bate 100% com o bloco nulo)
# 
# Será que o mesmo padrão se repete no bloco de cargo/senioridade e nos blocos
# técnicos (tipo "quais bancos de dados você usa")? Vou testar com uma coluna de
# exemplo de cada.

# In[12]:


# Testando se o nulo de cargo/senioridade e dos blocos técnicos segue o mesmo padrão
col_cargo = mapa_colunas["cargo_atual"]
exemplo_bloco_tecnico = obter_bloco(mapa_blocos["banco_dados"])[0]

df.select(
    col_segura(col_cargo).isNull().alias("cargo_nulo"),
    col_segura(exemplo_bloco_tecnico).alias("valor_bloco"),
).groupBy("cargo_nulo", "valor_bloco").count().show()


# Confirmado - quem não tem `cargo_atual` preenchido também não respondeu nada
# do bloco técnico (o valor vem nulo junto). Agora quero testar uma hipótese
# parecida pros blocos ligados à gestão de time: será que só quem respondeu
# "Gestor? = Sim" preenche essas perguntas?

# In[13]:


col_gestor = mapa_colunas["atua_como_gestor"]
exemplo_bloco_gestor = obter_bloco(mapa_blocos["cargos_no_time_dados"])[0]

df.select(
    col_segura(col_gestor).alias("gestor"),
    col_segura(exemplo_bloco_gestor).alias("valor_bloco"),
).groupBy("gestor", "valor_bloco").count().show()


# Confirmado de novo: os blocos `cargos_no_time_dados`, `desafios_gestor` e
# `ia_motivos_nao_uso` só têm valor preenchido pra quem marcou "Gestor? = Sim".
# Todo mundo que não é gestor fica nulo aí, pelo mesmo motivo de sempre -
# pergunta condicional, não erro.
# 
# Antes de fechar essa investigação, reparei numa coluna que não se encaixou em
# nenhum dos 3 grupos que encontrei até aqui: `objetivo_carreira`. Ela tem
# muitíssimo nulo (quase toda a base), bem mais que os outros grupos - o que
# sugere que o gatilho dela é outro. Vou conferir contra a situação de trabalho,
# igual fiz com o primeiro grupo.

# In[14]:


col_objetivo = mapa_colunas["objetivo_carreira"]

df.select(
    col_segura(col_situacao),
    col_segura(col_objetivo).isNotNull().alias("respondeu_objetivo"),
).groupBy(col_situacao, "respondeu_objetivo").count().orderBy(col_situacao).show(truncate=False)


# Interessante - é o **oposto exato** do primeiro grupo que encontrei. Quem
# respondeu `objetivo_carreira` é só quem está **fora do mercado de trabalho
# ativo** (desempregado, estudante, área acadêmica) - ou seja, a mesma lista de
# situações de `NAO_APLICA_EMPREGO`, só que invertida. Faz sentido pela lógica
# da pesquisa: pra quem já está empregado, pergunta-se sobre a empresa atual;
# pra quem não está, pergunta-se sobre o objetivo de entrada/retorno à área.
# 
# **Resumindo o que descobri sobre os nulos:** existem 4 grupos de perguntas
# condicionais na pesquisa:
# 1. Bloco de emprego atual (setor, salário, experiência, modelo de trabalho,
#    satisfação) → só quem está com vínculo empregatício ativo responde.
# 2. Bloco técnico (cargo, senioridade, linguagens, bancos de dados, fontes de
#    dados, cloud, BI) → só quem tem `cargo_atual` preenchido responde.
# 3. Bloco de gestão (cargos no time, desafios de gestor, motivos de não usar
#    IA) → só quem respondeu "Gestor? = Sim" responde.
# 4. Objetivo de carreira → só quem está **fora** do mercado de trabalho ativo
#    responde (o inverso exato do grupo 1).
# 
# ## 7. Decidindo como tratar esses nulos "estruturais"
# 
# Pensei em fazer como eu costumo fazer nesse tipo de caso: preencher cada
# coluna nula com um rótulo de texto tipo `"Não se aplica"`. Só que aí percebi
# um problema - toda vez que eu (ou qualquer um do grupo) for escrever uma
# consulta SQL filtrando só quem está empregado, ia precisar repetir um
# `WHERE coluna != 'Não se aplica'` em cada query, e ainda corria o risco de
# esquecer alguma coluna.
# 
# Decidi fazer diferente: criar **4 colunas booleanas de escopo**, uma pra cada
# grupo de pergunta condicional que encontrei (a quarta é literalmente o inverso
# da primeira, já que o gatilho de `objetivo_carreira` é "não estar empregado").
# Assim, o filtro em qualquer análise fica só `WHERE aplica_analise_emprego =
# true`, sem precisar repetir a lista de "não se aplica" toda vez.

# In[15]:


# Lista de situações em que o respondente não deve ser incluído na análise de emprego
NAO_APLICA_EMPREGO = [
    "Desempregado, buscando recolocação",
    "Desempregado e não estou buscando recolocação",
    "Somente Estudante (graduação)",
    "Somente Estudante (pós-graduação)",
    "Trabalho na área Acadêmica/Pesquisador",
]

df = (
    df
    .withColumn("aplica_analise_emprego", ~col_segura(col_situacao).isin(*NAO_APLICA_EMPREGO))
    .withColumn("aplica_analise_tecnica", col_segura(mapa_colunas["cargo_atual"]).isNotNull())
    .withColumn("aplica_analise_gestor", col_segura(mapa_colunas["atua_como_gestor"]) == "1")
)
df = df.withColumn("aplica_analise_busca_oportunidade", ~F.col("aplica_analise_emprego"))

print("Flags de escopo criadas: aplica_analise_emprego, aplica_analise_tecnica, "
      "aplica_analise_gestor, aplica_analise_busca_oportunidade")


# ## 8. Procurando inconsistências nos valores
# 
# Nulo tratado, mas ainda preciso conferir se os *valores* fazem sentido.
# Comecei pela faixa salarial, olhando as categorias com menos gente (às vezes é
# aí que mora o erro de digitação).

# In[16]:


df.groupBy(col_segura(col_salario)).count().orderBy("count").show(5, truncate=False)


# **Achado:** existe uma categoria `"de R$ 101/mês a R$ 2.000/mês"` com só 1
# pessoa, que não segue o padrão das outras faixas (todas em intervalos de
# R$1.000 em R$1.000, tipo "1.001 a 2.000", "2.001 a 3.000"...). Tem cara de
# erro de digitação no formulário original - bem provável que devesse ser "de
# R$ 1.001/mês a R$ 2.000/mês" e faltou o "1." na hora de configurar essa opção
# na pesquisa.
# 
# Pensei em simplesmente descartar essa linha (é só 1 pessoa em ~5300, o impacto
# estatístico é zero de qualquer jeito). Mas decidi não fazer isso: a pessoa
# respondeu a pesquisa de verdade, e o erro parece ser do formulário, não dela -
# descartar jogaria fora uma resposta válida por causa de um provável typo que
# não foi culpa do respondente. Preferi corrigir pra faixa mais próxima, que é a
# leitura mais óbvia da intenção original.

# In[17]:


VALOR_INVALIDO_SALARIO = "de R$ 101/mês a R$ 2.000/mês"
VALOR_CORRIGIDO_SALARIO = "de R$ 1.001/mês a R$ 2.000/mês"

df = df.withColumn(
    col_salario,
    F.when(col_segura(col_salario) == VALOR_INVALIDO_SALARIO, VALOR_CORRIGIDO_SALARIO)
     .otherwise(col_segura(col_salario))
)


# Já que estou revisando categoria por categoria, deixa eu conferir outra
# coluna parecida: `num_funcionarios` (porte da empresa). Também é uma pergunta
# de faixa, mesmo estilo da faixa salarial - vale a pena o mesmo cuidado.

# In[18]:


col_num_funcionarios = mapa_colunas["num_funcionarios"]
df.groupBy(col_segura(col_num_funcionarios)).count().orderBy("count").show(10, truncate=False)


# Achei outra categoria fora do padrão: `"de 501 a 100"`, com 1 pessoa. As
# faixas "corretas" dessa pergunta não se sobrepõem (1 a 5, 6 a 10, 11 a 50, 51
# a 100, 101 a 500, 501 a 1.000, 1.001 a 3.000, Acima de 3.000) - então dá pra
# comparar com a faixa salarial e ver se o mesmo tipo de correção (juntar na
# faixa mais próxima) se aplica aqui também.
# 
# Só que dessa vez a resposta não é óbvia como foi lá. Reparando com calma:
# - O **começo** `"501"` bate exatamente com a faixa `"de 501 a 1.000"`.
# - O **fim** `"100"` bate exatamente com a faixa `"de 51 a 100"`.
# 
# Ou seja, essa string parece ter juntado o começo de uma faixa com o fim de
# outra - não dá pra saber com confiança qual das duas a pessoa quis dizer, e
# "51 a 100 funcionários" e "501 a 1.000 funcionários" são portes de empresa bem
# diferentes (pequena vs. média/grande). Diferente da faixa salarial (onde só
# existia 1 correção plausível), aqui **corrigir errado é pior que deixar
# nulo** - eu estaria inventando um dado que pode não ser verdade. Vou marcar
# como nulo em vez de arriscar uma correção às cegas.

# In[19]:


VALOR_INVALIDO_PORTE = "de 501 a 100"

df = df.withColumn(
    col_num_funcionarios,
    F.when(col_segura(col_num_funcionarios) == VALOR_INVALIDO_PORTE, None)
     .otherwise(col_segura(col_num_funcionarios))
)

print("Categoria inválida de porte de empresa tratada como nulo (ambígua demais pra corrigir com confiança).")


# In[20]:


### Mais uma coluna de faixa pra conferir: tempo_experiencia_dados

col_exp_dados = mapa_colunas["tempo_experiencia_dados"]
df.groupBy(col_segura(col_exp_dados)).count().orderBy("count").show(10, truncate=False)


# Achado: existem duas categorias que se sobrepõem — "de 4 a 6 anos" (463 pessoas) e "de 5 a 6 anos" (356 pessoas). Fui conferir a pergunta irmã (tempo_experiencia_ti, que pergunta a mesma coisa só que sobre TI antes de entrar em dados) e ela não tem a opção "de 4 a 6 anos" — só tem "de 5 a 6 anos", numa progressão limpa (Menos de 1, 1-2, 3-4, 5-6, 7-10, Mais de 10). Isso indica que "de 4 a 6 anos" é uma opção espúria que ficou só na pergunta de experiência em dados, sobrepondo uma faixa que já existia. Como as duas faixas se confundem (quem tem 5 anos poderia ter marcado qualquer uma), decidi juntar as duas na categoria que também aparece na pergunta irmã, preservando todo mundo:

# In[21]:


df = df.withColumn(
    col_exp_dados,
    F.when(col_segura(col_exp_dados) == "de 4 a 6 anos", "de 5 a 6 anos")
     .otherwise(col_segura(col_exp_dados))
)

print("Categoria 'de 4 a 6 anos' unificada com 'de 5 a 6 anos'.")


# Agora quero conferir uma coluna que devia ser simples de "sim/não":
# `satisfeito_empresa`. Antes de mexer, sempre vale checar o tipo e os valores
# distintos, em vez de assumir.

# In[22]:


col_satisfeito = mapa_colunas["satisfeito_empresa"]
print(df.select(col_segura(col_satisfeito)).dtypes)
df.groupBy(col_segura(col_satisfeito)).count().show()


# **Achado importante:** essa coluna não veio como boolean - veio como **texto**
# `"1"`/`"0"`. Isso é uma pegadinha: se eu tivesse assumido que "nulo = não
# marcou" e usado só `isNotNull()`, eu estaria errado, porque o "não" também
# está preenchido (com o texto `"0"`), só que ele *parece* fraco/vazio à
# primeira vista. Preciso converter explicitamente pra boolean de verdade.

# In[23]:


df = df.withColumn(
    col_satisfeito,
    F.when(col_segura(col_satisfeito) == "1", True)
     .when(col_segura(col_satisfeito) == "0", False)
     .otherwise(None).cast("boolean")
)


# Será que os blocos de múltipla escolha (tipo "quais bancos de dados você usa")
# têm o mesmo problema? Vou conferir um exemplo antes de tratar todos de uma vez.

# In[24]:


exemplo_bloco = obter_bloco(mapa_blocos["banco_dados"])[0]
print(df.select(col_segura(exemplo_bloco)).dtypes)
df.groupBy(col_segura(exemplo_bloco)).count().show()


# Mesma história: vem como string `"1"`/`"0"`, não como boolean nem como
# nulo/preenchido. Ou seja, essa base tem esse padrão em várias colunas
# diferentes - vale a pena eu tratar de forma sistemática em vez de coluna por
# coluna.
# 
# Mais um detalhe importante aqui: preciso lembrar do escopo que descobri lá
# atrás. A maioria dos blocos só vale pra quem tem `cargo_atual` preenchido, mas
# 3 blocos (`cargos_no_time_dados`, `desafios_gestor`, `ia_motivos_nao_uso`) só
# valem pra quem é gestor. Vou escrever uma primeira versão da função que já
# respeita isso na hora de converter.

# In[25]:


BLOCOS_ESCOPO_GESTOR = {"cargos_no_time_dados", "desafios_gestor", "ia_motivos_nao_uso"}

def padronizar_bloco_v1(df, nome_bloco, codigo_bloco):
    col_escopo = "aplica_analise_gestor" if nome_bloco in BLOCOS_ESCOPO_GESTOR else "aplica_analise_tecnica"
    for col in obter_bloco(codigo_bloco):
        valor = col_segura(col).cast("int")
        df = df.withColumn(
            col,
            F.when(col_segura(col_escopo) == False, None)
             .when(valor == 1, True)
             .when(valor == 0, False)
             .otherwise(None)
        )
    return df


# Antes de aplicar essa função de uma vez pros 12 blocos e seguir em frente, quero
# fazer uma varredura: só testei a regra em 2 blocos de exemplo
# (`banco_dados` e `cargos_no_time_dados`). Será que ela vale igual pros outros
# 10? Vou comparar, pra cada bloco, quantas pessoas responderam alguma coisa
# *dentro* do escopo que assumi contra quantas responderam *fora* dele - se a
# regra estiver certa, "fora do escopo" deveria dar sempre zero.

# In[26]:


for nome_bloco, codigo in mapa_blocos.items():
    col_escopo = "aplica_analise_gestor" if nome_bloco in BLOCOS_ESCOPO_GESTOR else "aplica_analise_tecnica"
    colunas_bloco = obter_bloco(codigo)
    respondeu = F.greatest(*[col_segura(c).isNotNull().cast("int") for c in colunas_bloco]) == 1

    print(f"--- {nome_bloco} (escopo assumido: {col_escopo}) ---")
    df.select(respondeu.alias("respondeu"), col_segura(col_escopo).alias("dentro_do_escopo")) \
      .groupBy("dentro_do_escopo", "respondeu").count() \
      .orderBy("dentro_do_escopo", "respondeu").show()


# Achei **2 vazamentos reais** que eu não tinha percebido quando só testei os 2
# blocos de exemplo:
# 
# **1. `motivo_insatisfacao`** - a regra que apliquei por padrão pra esse bloco
# foi `aplica_analise_tecnica` (ter cargo preenchido), só que isso está errado.
# Reparando melhor na pergunta, o gatilho real é outro: só quem respondeu
# **"Você está satisfeito na sua empresa atual? = Não"** recebe essa pergunta -
# faz todo sentido, a pesquisa só pergunta o motivo da insatisfação pra quem
# está insatisfeito. Vou confirmar isso direto.

# In[27]:


col_motivo_exemplo = obter_bloco(mapa_blocos["motivo_insatisfacao"])[0]

df.select(
    col_segura(col_satisfeito).alias("satisfeito"),
    col_segura(col_motivo_exemplo).isNotNull().alias("respondeu_motivo"),
).groupBy("satisfeito", "respondeu_motivo").count().show()


# Confirmado: **todo mundo** que respondeu alguma coisa em `motivo_insatisfacao`
# tem `satisfeito_empresa = Não` (nenhuma exceção). O escopo certo não é
# `aplica_analise_tecnica`, é `satisfeito_empresa == False`. Com a regra errada
# que eu tinha, **213 respostas válidas** de gente insatisfeita que não tinha
# `cargo_atual` preenchido estavam sendo zeradas à toa - só porque essas pessoas
# não bateram no critério errado que eu apliquei.
# 
# **2. `experiencia_prejudicada`** - esse bloco é do P1 (perfil), pergunta feita
# logo no início da pesquisa, bem antes de qualquer pergunta sobre emprego atual.
# Não devia ter filtro de escopo nenhum - é feita a todo mundo, empregado,
# desempregado ou estudante. Vou confirmar olhando a taxa de resposta por
# situação de trabalho.

# In[28]:


col_exp_exemplo = obter_bloco(mapa_blocos["experiencia_prejudicada"])[0]

df.select(
    col_segura(col_situacao).alias("situacao_trabalho"),
    col_segura(col_exp_exemplo).isNotNull().alias("respondeu_experiencia"),
).groupBy("situacao_trabalho").pivot("respondeu_experiencia").count().show(truncate=False)


# Confirmado: gente de **todas** as situações de trabalho respondeu essa
# pergunta, inclusive desempregado e estudante - não é uma pergunta condicionada
# a ter cargo. A regra errada estava zerando **714 respostas válidas** de gente
# sem `cargo_atual` preenchido.
# 
# ## Corrigindo a função
# 
# Agora sim, com os 2 casos especiais mapeados, escrevo a versão definitiva:
# - `motivo_insatisfacao` usa como escopo `satisfeito_empresa == False`.
# - `experiencia_prejudicada` não usa filtro de escopo nenhum.
# - Os outros 10 blocos seguem a regra original (`aplica_analise_gestor` pros 3
#   blocos de gestão, `aplica_analise_tecnica` pro resto).
# 
#   **Nota de organização:** a partir daqui, separei o que é mecânica genérica
# do que é regra de negócio desta edição. A conversão de texto `'1'`/`'0'`
# pra boolean, respeitando um escopo, virou `padronizar_bloco` em
# `utils/functions.py` (mesma função pras 3 edições). O que continua aqui,
# local, é o **mapeamento de negócio** — `escopo_do_bloco` e
# `BLOCOS_ESCOPO_GESTOR` — porque isso é fruto da investigação de nulo desta
# base especificamente (2023), e pode não valer 1:1 pras edições 2024 e 2025
#  se a pesquisa mudar de formato entre anos.

# In[29]:


BLOCOS_SEM_ESCOPO = {"experiencia_prejudicada"}

def escopo_do_bloco(nome_bloco):
    """Devolve a condição (Column) que diz se a linha deveria ter resposta nesse
    bloco, ou None se o bloco não tem restrição de escopo nenhuma."""
    if nome_bloco in BLOCOS_SEM_ESCOPO:
        return None
    if nome_bloco == "motivo_insatisfacao":
        return col_segura(col_satisfeito) == False
    if nome_bloco in BLOCOS_ESCOPO_GESTOR:
        return F.col("aplica_analise_gestor")
    return F.col("aplica_analise_tecnica")

def padronizar_bloco(df, nome_bloco, codigo_bloco):
    return _padronizar_bloco(df, nome_bloco, codigo_bloco, mapa_codigo_para_nome, escopo_do_bloco(nome_bloco))

for nome_bloco, codigo in mapa_blocos.items():
    df = padronizar_bloco(df, nome_bloco, codigo)

print("Blocos padronizados para boolean, respeitando o escopo certo de cada um "
      "(incluindo os 2 casos especiais que encontrei na varredura).")


# ## 9. Regras de negócio — resumo de referência
# 
# Antes de montar a tabela final, quero deixar tudo que descobri sobre escopo e
# agregação registrado num lugar só, fácil de consultar - sem precisar reler
# toda a investigação toda vez que alguém (inclusive eu, num notebook futuro)
# precisar lembrar qual filtro usar em qual coluna.
# 
# **Filtros de escopo**
# 
# | Regra | Descrição | Aplica em |
# | :--- | :--- | :--- |
# | `aplica_analise_emprego = true` | Exclui desempregados, apenas estudantes ou área acadêmica. | `setor_empresa`, `faixa_salarial`, `tempo_experiencia_dados`, `tempo_experiencia_ti`, `modelo_trabalho_atual`, `modelo_trabalho_ideal`, `satisfeito_empresa` |
# | `aplica_analise_tecnica = true` | Exclui quem não tem `cargo_atual` preenchido. | `cargo_atual`, `nivel_senioridade` e os blocos técnicos (`linguagens_trabalho`, `banco_dados`, `fontes_dados`, `cloud`, `ferramenta_bi`, `ia_tipo_uso`) |
# | `aplica_analise_gestor = true` | Só quem respondeu `atua_como_gestor = Sim`. | `cargos_no_time_dados`, `desafios_gestor`, `ia_motivos_nao_uso` |
# | `aplica_analise_busca_oportunidade = true` | O inverso exato de `aplica_analise_emprego` - só quem está fora do mercado de trabalho ativo. | `objetivo_carreira` |
# | `satisfeito_empresa = false` | Caso especial: gatilho não é escopo geral, é a resposta de outra pergunta. | `motivo_insatisfacao` |
# | *(sem filtro)* | Pergunta de perfil (bloco P1), respondida por todo mundo, independente de situação de trabalho. | `experiencia_prejudicada` |
# 
# **Nota sobre `ia_tipo_uso`:** na teoria, o gatilho certo dessa pergunta é "não
# ser gestor" (`aplica_analise_gestor = false`). Na prática, usei
# `aplica_analise_tecnica` mesmo assim, porque descobri que nessa base as duas
# condições descrevem exatamente o mesmo grupo de gente: quem é gestor responde
# uma pergunta de cargo diferente (`"Cargo como Gestor"`, não capturada nessa
# Silver), então `cargo_atual` preenchido e "não ser gestor" acabam sendo
# sinônimos aqui. Funciona, mas é bom deixar registrado o porquê, pra não
# parecer coincidência da próxima vez que alguém for mexer nisso.
# 
# **Regra de agregação (múltipla escolha)**
# 
# Colunas dos blocos viraram boolean (`true`/`false`), não nulo/preenchido. Pra
# contar quantas pessoas marcaram uma opção, uso soma condicional, nunca
# `COUNT`:
# 
# ```sql
# -- Correto: conta só quem marcou true
# SUM(CASE WHEN coluna THEN 1 ELSE 0 END) AS total
# 
# -- Errado: COUNT considera qualquer valor não-nulo, incluindo os `false`
# COUNT(coluna)
# ```
# 
# **Cuidado com viés de composição:** ao comparar médias entre grupos (gênero,
# região, etc.), sempre olhar também aberto por cargo/senioridade antes de
# confiar na média simples - um grupo com mais gente Sênior sai com média mais
# alta só por causa disso, não por um efeito real do grupo.

# ## 10. Montando e exportando a tabela Silver final

# In[30]:


selects = [col_segura(coluna).alias(conceito) for conceito, coluna in mapa_colunas.items()]
for nome_bloco, codigo in mapa_blocos.items():
    for c in obter_bloco(codigo):
        selects.append(col_segura(c).alias(f"{nome_bloco}__{slug(c)}"))
selects += [F.col("aplica_analise_emprego"), F.col("aplica_analise_tecnica"),
            F.col("aplica_analise_gestor"), F.col("aplica_analise_busca_oportunidade"),
            F.col("ano_pesquisa")]

df_silver = df.select(*selects)
print(f"Silver final: {len(df_silver.columns)} colunas")
df_silver.printSchema()


# Salvando em Parquet (formato oficial da camada Silver no Data Lake, colunar,
# mais eficiente), particionado por `ano_pesquisa` -- mesmo padrão da Bronze,
# já que Carlos/Maycon/Vini escrevem nesse mesmo caminho, cada um na sua
# partição.

# In[ ]:


df_silver.write \
    .mode("overwrite") \
    .partitionBy("ano_pesquisa") \
    .parquet(CAMINHO_SILVER)

print(f"Silver exportada com sucesso em: {CAMINHO_SILVER}")
print(f"Linhas: {df_silver.count()} | Colunas: {len(df_silver.columns)}")


# ## 11. Resumo do que descobri
# 
# Juntando tudo:
# 
# 1. **Nomes de coluna**: vinham em formato de tupla, com 2 casos que quebravam
#    uma regex mais simples (apóstrofo na descrição e uma aspa faltando no CSV
#    original) - resolvido com uma regex mais tolerante, testada contra as 399
#    colunas.
# 2. **Nulos**: não tinha nada faltando por erro - são 4 grupos de perguntas
#    condicionais (emprego atual, bloco técnico, bloco de gestão, e objetivo de
#    carreira - esse último com o gatilho invertido: só responde quem *não* está
#    empregado), todos confirmados cruzando com a coluna que "libera" aquele
#    bloco. Tratei com 4 colunas de escopo booleanas em vez de rótulo de texto,
#    pra facilitar filtro em SQL.
# 3. **Faixa salarial**: 1 categoria fora do padrão, corrigida pra faixa mais
#    próxima (decidi preservar a resposta em vez de descartar).
# 3b.**Porte da empresa**: 1 categoria fora do padrão (`"de 501 a 100"`),
#    parecendo ter juntado o começo de uma faixa com o fim de outra. Diferente
#    da faixa salarial, aqui não tinha uma correção única e óbvia - "51 a 100"
#    e "501 a 1.000" são portes bem diferentes, e arriscar a correção errada
#    seria pior que deixar nulo. Marquei como nulo em vez de adivinhar.
# 4. **Colunas de sim/não e de múltipla escolha**: não vinham como boolean de
#    verdade, vinham como texto `"1"`/`"0"` - convertido explicitamente,
#    respeitando o escopo certo de cada bloco.
# 5. **Vazamento de escopo em 2 blocos**: quando fiz a varredura comparando os
#    12 blocos contra a regra de escopo que eu tinha assumido, achei que
#    `motivo_insatisfacao` estava usando o critério errado (o certo é
#    `satisfeito_empresa = Não`, não ter cargo preenchido - isso zerava 213
#    respostas válidas) e que `experiencia_prejudicada` não deveria ter filtro
#    de escopo nenhum, por ser pergunta de perfil feita a todo mundo (a regra
#    errada zerava 714 respostas válidas). Os dois foram corrigidos.
# 6. **Dicionário de colunas**: gerei um `dict_colums_2023.py` com o de-para
#    completo, organizado pelas 7 perguntas do desafio, pra não precisar
#    redigitar esse mapeamento em cada análise nova.
# 
# 
# **Próximos passos:** rodar esse notebook inteiro no AWS Academy Lab (Glue
# Notebook ou EMR), conferir se os números batem com o que já vi aqui, subir a
# Silver pro S3 e catalogar no Glue, e usar o `dict_colums_2023.py` gerado como
# referência nas próximas análises do grupo.
