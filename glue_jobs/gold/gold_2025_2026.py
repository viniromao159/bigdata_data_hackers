#!/usr/bin/env python
# coding: utf-8

# # State of Data Brasil 2025 — Gold
# ### Tech Challenge Fase 3 — Grupo 6
# ### Edição 2025
# 
# Fonte: [Kaggle — State of Data Brasil](https://www.kaggle.com/datahackers/datasets)
# 
# A Gold é a camada de **consumo**: responde as 7 perguntas de negócio do desafio a
# partir da Silver e persiste cada resposta como uma tabela pronta pra gráfico. Cada
# tabela sai particionada por `ano_pesquisa`, no mesmo caminho compartilhado — assim
# o material executivo lê a Gold das edições juntas e compara ano a ano.
# 
# ## 1. Preparando para as consultas SQL

# In[ ]:


BUCKET = "s3://state-of-data-2023-1819-2244-3791"
CAMINHO_SILVER = f"{BUCKET}/data/silver/state_of_data_silver"
CAMINHO_GOLD_BASE = f"{BUCKET}/data/gold"

print("Silver:", CAMINHO_SILVER)
print("Gold:", CAMINHO_GOLD_BASE)

# Versão de teste no Glue: utils/constants.py não está disponível no cluster,
# então colei as constantes direto aqui por enquanto.
ponto_medio_salarial = {
    "Menos de R$ 1.000/mês": 750,
    "de R$ 1.001/mês a R$ 2.000/mês": 1500,
    "de R$ 2.001/mês a R$ 3.000/mês": 2500,
    "de R$ 3.001/mês a R$ 4.000/mês": 3500,
    "de R$ 4.001/mês a R$ 6.000/mês": 5000,
    "de R$ 6.001/mês a R$ 8.000/mês": 7000,
    "de R$ 8.001/mês a R$ 12.000/mês": 10000,
    "de R$ 12.001/mês a R$ 16.000/mês": 14000,
    "de R$ 16.001/mês a R$ 20.000/mês": 18000,
    "de R$ 20.001/mês a R$ 25.000/mês": 22500,
    "de R$ 25.001/mês a R$ 30.000/mês": 27500,
    "de R$ 30.001/mês a R$ 40.000/mês": 35000,
    "Acima de R$ 40.001/mês": 45000,
}

# 2025 tem um nível de senioridade novo que não existia em 2023
ordem_senioridade = ["Júnior", "Pleno", "Sênior", "Especialista/Staff+"]

# 2025 não repete o problema de 2023 (categorias "de 4 a 6 anos"/"de 5 a 6 anos"
# sobrepostas na Silver) -- lista sem a categoria espúria
ordem_tempo_experiencia = [
    "Não tenho experiência na área de dados",
    "Não tive experiência na área de TI/Engenharia de Software antes de "
    "começar a trabalhar na área de dados",
    "Menos de 1 ano",
    "de 1 a 2 anos",
    "de 3 a 4 anos",
    "de 5 a 6 anos",
    "de 7 a 10 anos",
    "Mais de 10 anos",
]

# In[ ]:


from pyspark.sql import SparkSession, functions as F

ANO_PESQUISA = 2025
spark = SparkSession.builder.appName(f"state-of-data-{ANO_PESQUISA}-gold").getOrCreate()
print(f"Ano de pesquisa: {ANO_PESQUISA}")


# Leio a Silver e filtro só a minha edição (o caminho é compartilhado, particionado
# por `ano_pesquisa`). Registro como view temporária pra consultar em SQL.
#
# `mergeSchema=true` é obrigatório aqui pelo mesmo motivo da Silver lendo a
# Bronze: a Silver 2023 e a Silver 2025 têm schemas diferentes (a 2025 tem
# colunas que a 2023 não tem, como as flags `aplica_bloco_engenharia_dados`/
# `analise_dados`/`ciencia_dados`). Sem essa opção, o Spark lê o schema de uma
# única partição (normalmente a que vem primeiro em ordem alfabética,
# `ano_pesquisa=2023`) e aplica ele em cima de todas as partições -- as colunas
# que só existem em 2025 sumiriam do DataFrame antes mesmo do filtro abaixo.

# In[ ]:


df_silver = (
    spark.read.option("mergeSchema", "true").parquet(str(CAMINHO_SILVER))
    .filter(F.col("ano_pesquisa") == ANO_PESQUISA)
)
print(f"{df_silver.count()} linhas, {len(df_silver.columns)} colunas")
df_silver.createOrReplaceTempView("state_of_data")


# ## 2. Regras de referência para as consultas
# 
# **Filtros de escopo** (colunas booleanas geradas na Silver — evitam repetir a
# lista de "não se aplica" em toda query):
# 
# | Flag | Usar em |
# | :-- | :-- |
# | `aplica_analise_emprego` | setor, salário, experiência, modelo de trabalho, satisfação |
# | `aplica_analise_tecnica` | cargo, senioridade e os blocos técnicos (linguagens, banco, cloud, BI, IA técnica) |
# | `aplica_analise_gestor` | cargos no time, desafios e responsabilidades de gestor |
# | `satisfeito_empresa = false` | motivo de insatisfação |
# 
# **Contagem de múltipla escolha:** as colunas de bloco são boolean. Pra contar quem
# marcou uma opção uso `SUM(CASE WHEN coluna THEN 1 ELSE 0 END)`, nunca `COUNT`
# (que contaria também os `false`).
# 
# **Viés de composição:** ao comparar médias salariais entre grupos (região, gênero),
# sempre abro também por senioridade — um grupo com mais gente sênior sobe a média
# por composição, não por um efeito real do grupo.

# A faixa salarial é uma categoria de texto. Pra calcular médias e comparar grupos,
# crio uma view auxiliar com a faixa convertida para o ponto médio em R$
# (`faixa_salarial_num`), usando o de-para de `utils/constants.py`.

# In[ ]:


case_faixa_salarial = " ".join(
    f"WHEN '{faixa}' THEN {valor}" for faixa, valor in ponto_medio_salarial.items()
)
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW state_of_data_num AS
    SELECT *,
        CASE faixa_salarial {case_faixa_salarial} ELSE NULL END AS faixa_salarial_num
    FROM state_of_data
""")
print("Views criadas: state_of_data e state_of_data_num")


# Função de gráfico de barras horizontais reutilizada em todas as perguntas (recebe
# um pandas vindo de `toPandas()`). Uso backend `Agg` pra o notebook rodar mesmo sem
# display (ex: execução automatizada).

# In[ ]:


import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import warnings
warnings.filterwarnings("ignore")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

def grafico_barh(pdf, col_categoria, col_valor, titulo, xlabel, cor="#2E86AB", figsize=(9, 5)):
    pdf = pdf.sort_values(col_valor)
    plt.figure(figsize=figsize)
    plt.barh(pdf[col_categoria].astype(str), pdf[col_valor], color=cor)
    plt.title(titulo)
    plt.xlabel(xlabel)
    plt.tight_layout()
    plt.show()

def soma_multipla_escolha(rotulos_colunas, escopo="aplica_analise_tecnica"):
    """Monta uma query UNION ALL somando cada opção de um bloco de múltipla
    escolha (boolean) numa linha (rotulo, total). Evita repetir SQL na mão."""
    partes = [
        f"SELECT '{rotulo}' AS categoria, "
        f"SUM(CASE WHEN {coluna} THEN 1 ELSE 0 END) AS total "
        f"FROM state_of_data WHERE {escopo} = true"
        for rotulo, coluna in rotulos_colunas
    ]
    return " UNION ALL ".join(partes) + " ORDER BY total DESC"


# ## 3. Respondendo as perguntas do Tech Challenge
# 
# ### P1 — Como está estruturado o mercado brasileiro de Dados?

# **1. Distribuição da situação de trabalho.** Pergunta de perfil, respondida por
# todos — sem filtro de escopo.

# In[ ]:


resultado_p1_1 = spark.sql("""
    SELECT situacao_trabalho, COUNT(*) AS total,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentual
    FROM state_of_data
    GROUP BY situacao_trabalho
    ORDER BY total DESC
""")
resultado_p1_1.show(truncate=False)
grafico_barh(resultado_p1_1.toPandas(), "situacao_trabalho", "total",
             "Situação de trabalho dos respondentes 2025", "Quantidade de pessoas")


# **2. Top 10 setores de atuação.** Setor é pergunta sobre a empresa atual, então
# filtro por `aplica_analise_emprego` (exclui quem não tem vínculo).

# In[ ]:


resultado_p1_2 = spark.sql("""
    SELECT setor_empresa, COUNT(*) AS total
    FROM state_of_data
    WHERE aplica_analise_emprego = true
    GROUP BY setor_empresa ORDER BY total DESC LIMIT 10
""")
resultado_p1_2.show(truncate=False)
grafico_barh(resultado_p1_2.toPandas(), "setor_empresa", "total",
             "Top 10 setores de atuação 2025", "Quantidade de pessoas")


# **3. Distribuição de cargos atuais.** Cargo só existe pra quem atua tecnicamente,
# então filtro por `aplica_analise_tecnica`.

# In[ ]:


resultado_p1_3 = spark.sql("""
    SELECT cargo_atual, COUNT(*) AS total
    FROM state_of_data WHERE aplica_analise_tecnica = true
    GROUP BY cargo_atual ORDER BY total DESC
""")
resultado_p1_3.show(truncate=False)
grafico_barh(resultado_p1_3.toPandas(), "cargo_atual", "total",
             "Distribuição de cargos 2025", "Total")


# **4. Distribuição de senioridade.** Mesmo escopo técnico. Nesta edição existe o
# nível "Especialista/Staff+" além de Júnior/Pleno/Sênior — a query não filtra
# níveis, então ele aparece no resultado.

# In[ ]:


resultado_p1_4 = spark.sql("""
    SELECT nivel_senioridade, COUNT(*) AS total,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 0) AS percentual
    FROM state_of_data WHERE aplica_analise_tecnica = true
    GROUP BY nivel_senioridade ORDER BY total DESC
""")
resultado_p1_4.show(truncate=False)
grafico_barh(resultado_p1_4.toPandas(), "nivel_senioridade", "total",
             "Distribuição de senioridade 2025", "Total")


# **5. % de profissionais que atuam como gestor.** Sobre a base empregada
# (`aplica_analise_emprego`), qual fração é gestora. Resultado vira um card.

# In[ ]:


resultado_p1_5 = spark.sql("""
    SELECT ROUND(100.0 * SUM(CASE WHEN aplica_analise_gestor THEN 1 ELSE 0 END) / COUNT(*), 0) AS pct_gestores
    FROM state_of_data WHERE aplica_analise_emprego = true
""")
resultado_p1_5.show(truncate=False)
pct_val = resultado_p1_5.collect()[0]["pct_gestores"]
fig, ax = plt.subplots(figsize=(4, 2.2)); ax.axis("off")
ax.add_patch(patches.FancyBboxPatch((0.05, 0.05), 0.9, 0.9, boxstyle="round,pad=0.03",
             facecolor="#F8F9FA", edgecolor="#D0D7DE", linewidth=1.5))
ax.text(0.5, 0.58, f"{int(pct_val)}%", fontsize=38, fontweight="bold", ha="center", va="center", color="#0969DA")
ax.text(0.5, 0.28, "% dos profissionais que atuam como gestores 2025", fontsize=9, ha="center", va="center", color="#57606A")
plt.tight_layout(); plt.show()


# **6. Relação tempo de experiência em dados x senioridade.** Cruza as duas colunas;
# escopo técnico + empregado.

# In[ ]:


resultado_p1_6 = spark.sql("""
    SELECT tempo_experiencia_dados, nivel_senioridade, COUNT(*) AS total
    FROM state_of_data
    WHERE aplica_analise_emprego = true AND aplica_analise_tecnica = true
    GROUP BY tempo_experiencia_dados, nivel_senioridade
    ORDER BY tempo_experiencia_dados, total DESC
""")
resultado_p1_6.show(50, truncate=False)


# **7. Modelo de trabalho atual e o ideal.** Duas tabelas (atual e desejado),
# ambas sobre a base empregada.

# In[ ]:


import textwrap
resultado_p1_7 = spark.sql("""
    SELECT modelo_trabalho_atual, COUNT(*) AS total
    FROM state_of_data WHERE aplica_analise_emprego = true
    GROUP BY modelo_trabalho_atual ORDER BY total DESC
""")
resultado_p1_7.show(truncate=False)
pdf = resultado_p1_7.toPandas()
pdf["fmt"] = pdf["modelo_trabalho_atual"].apply(lambda x: textwrap.fill(str(x), 30))
grafico_barh(pdf, "fmt", "total", "Modelo de trabalho atual 2025", "Quantidade de pessoas")


# In[ ]:


resultado_p1_7_1 = spark.sql("""
    SELECT modelo_trabalho_ideal, COUNT(*) AS total
    FROM state_of_data WHERE aplica_analise_emprego = true
    GROUP BY modelo_trabalho_ideal ORDER BY total DESC
""")
resultado_p1_7_1.show(truncate=False)
pdf = resultado_p1_7_1.toPandas()
pdf["fmt"] = pdf["modelo_trabalho_ideal"].apply(lambda x: textwrap.fill(str(x), 30))
grafico_barh(pdf, "fmt", "total", "Modelo de trabalho ideal 2025", "Quantidade de pessoas")


# **8. Porte das empresas (nº de funcionários).** Sobre a base empregada; ignoro
# nulos (inclui a categoria ambígua que a Silver zerou).

# In[ ]:


resultado_p1_8 = spark.sql("""
    SELECT num_funcionarios, COUNT(*) AS total
    FROM state_of_data
    WHERE aplica_analise_emprego = true AND num_funcionarios IS NOT NULL
    GROUP BY num_funcionarios ORDER BY total DESC
""")
resultado_p1_8.show(20, truncate=False)
grafico_barh(resultado_p1_8.toPandas(), "num_funcionarios", "total",
             "Porte da empresa 2025", "Quantidade de pessoas")


# ### P2 — Quais perfis são mais valorizados?
# 
# **1. Faixa salarial por cargo e senioridade** (escopo empregado + técnico).

# In[ ]:


resultado_p2_1 = spark.sql("""
    SELECT cargo_atual, nivel_senioridade, faixa_salarial, COUNT(*) AS total
    FROM state_of_data
    WHERE aplica_analise_emprego = true AND aplica_analise_tecnica = true
    GROUP BY cargo_atual, nivel_senioridade, faixa_salarial
    ORDER BY cargo_atual, nivel_senioridade, total DESC
""")
resultado_p2_1.show(100, truncate=False)


# **2. Salário médio estimado por tempo de experiência (Dados e TI).** Uso a faixa
# convertida em R$ e ordeno pelas faixas de tempo (de `utils/constants.py`).

# In[ ]:


import textwrap
case_ordem_exp = " ".join(f"WHEN '{v}' THEN {i}" for i, v in enumerate(ordem_tempo_experiencia))

resultado_p2_2 = spark.sql(f"""
    SELECT tempo_experiencia_dados, ROUND(AVG(faixa_salarial_num), 0) AS salario_medio_estimado
    FROM state_of_data_num WHERE aplica_analise_emprego = true
    GROUP BY tempo_experiencia_dados
    ORDER BY CASE tempo_experiencia_dados {case_ordem_exp} ELSE 99 END ASC
""")
resultado_p2_2.show(truncate=False)
pdf = resultado_p2_2.toPandas()
pdf["fmt"] = pdf["tempo_experiencia_dados"].apply(lambda x: textwrap.fill(str(x), 30))
grafico_barh(pdf, "fmt", "salario_medio_estimado",
             "Salário médio por experiência em dados 2025", "Salário médio estimado (R$)")

resultado_p2_2_1 = spark.sql(f"""
    SELECT tempo_experiencia_ti, ROUND(AVG(faixa_salarial_num), 0) AS salario_medio_estimado
    FROM state_of_data_num WHERE aplica_analise_emprego = true
    GROUP BY tempo_experiencia_ti
    ORDER BY CASE tempo_experiencia_ti {case_ordem_exp} ELSE 99 END ASC
""")
resultado_p2_2_1.show(truncate=False)
pdf = resultado_p2_2_1.toPandas()
pdf["fmt"] = pdf["tempo_experiencia_ti"].apply(lambda x: textwrap.fill(str(x), 30))
grafico_barh(pdf, "fmt", "salario_medio_estimado",
             "Salário médio por experiência em TI 2025", "Salário médio estimado (R$)")


# **3. Migrou de TI x começou direto em dados.** Uso o texto exato de "não tive
# experiência em TI" pra separar os dois grupos e comparar o salário médio.

# In[ ]:


resultado_p2_3 = spark.sql("""
    SELECT
        CASE WHEN tempo_experiencia_ti =
            'Não tive experiência na área de TI/Engenharia de Software antes de começar a trabalhar na área de dados'
            THEN 'Começou direto em dados' ELSE 'Migrou de TI' END AS origem,
        ROUND(AVG(faixa_salarial_num), 0) AS salario_medio_estimado, COUNT(*) AS total
    FROM state_of_data_num
    WHERE aplica_analise_emprego = true AND tempo_experiencia_ti IS NOT NULL
    GROUP BY 1 ORDER BY total DESC
""")
resultado_p2_3.show(truncate=False)
grafico_barh(resultado_p2_3.toPandas(), "origem", "salario_medio_estimado",
             "Salário médio por origem na área 2025", "Salário médio estimado (R$)")


# **4. Salário por função (Eng./Analista/Cientista) e senioridade.** Filtro os três
# cargos principais e ordeno a senioridade logicamente pro gráfico.

# In[ ]:


import textwrap, pandas as pd
resultado_p2_4 = spark.sql("""
    SELECT cargo_atual, nivel_senioridade, ROUND(AVG(faixa_salarial_num), 0) AS salario_medio_estimado
    FROM state_of_data_num
    WHERE aplica_analise_emprego = true AND aplica_analise_tecnica = true
      AND cargo_atual IN (
          'Engenheiro de Dados/Data Engineer/Data Architect',
          'Analista de Dados/Data Analyst',
          'Cientista de Dados/Data Scientist')
    GROUP BY cargo_atual, nivel_senioridade
""")
resultado_p2_4.show(truncate=False)
pdf = resultado_p2_4.toPandas()
mapa = {'Analista de Dados/Data Analyst': 'Analista de Dados',
        'Cientista de Dados/Data Scientist': 'Cientista de Dados',
        'Engenheiro de Dados/Data Engineer/Data Architect': 'Engenheiro de Dados'}
pdf["cargo_curto"] = pdf["cargo_atual"].map(mapa)
pdf["nivel_senioridade"] = pd.Categorical(pdf["nivel_senioridade"], categories=ordem_senioridade, ordered=True)
pdf = pdf.sort_values(["cargo_curto", "nivel_senioridade"])
pdf["cargo_senioridade"] = pdf["cargo_curto"] + " - " + pdf["nivel_senioridade"].astype(str)
pdf["fmt"] = pdf["cargo_senioridade"].apply(lambda x: textwrap.fill(str(x), 30))
grafico_barh(pdf, "fmt", "salario_medio_estimado",
             "Salário médio por cargo e senioridade 2025", "Salário médio estimado (R$)")


# **5. Objetivos de carreira mais citados (top 5).** Esta pergunta é respondida
# sobretudo por quem está fora do mercado ativo, então uso
# `aplica_analise_emprego = false`.

# In[ ]:


resultado_p2_5 = spark.sql("""
    SELECT objetivo_carreira, COUNT(*) AS total
    FROM state_of_data
    WHERE aplica_analise_emprego = false AND objetivo_carreira IS NOT NULL
    GROUP BY objetivo_carreira ORDER BY total DESC LIMIT 5
""")
resultado_p2_5.show(truncate=False)


# ### P3 — Diversidade de gênero
# 
# **1. Proporção de gênero** (perfil, sem escopo).

# In[ ]:


resultado_p3_1 = spark.sql("""
    SELECT genero, COUNT(*) AS total,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentual
    FROM state_of_data GROUP BY genero ORDER BY total DESC
""")
resultado_p3_1.show(truncate=False)
grafico_barh(resultado_p3_1.toPandas(), "genero", "total", "Distribuição de gênero 2025", "Total")


# **2. Gap salarial por gênero no mesmo cargo e senioridade** (controla cargo +
# senioridade pra isolar o efeito do gênero).

# In[ ]:


resultado_p3_2 = spark.sql("""
    SELECT cargo_atual, nivel_senioridade, genero, ROUND(AVG(faixa_salarial_num), 0) AS salario_medio_estimado
    FROM state_of_data_num
    WHERE aplica_analise_emprego = true AND aplica_analise_tecnica = true
    GROUP BY cargo_atual, nivel_senioridade, genero
    ORDER BY cargo_atual, nivel_senioridade, genero
""")
resultado_p3_2.show(100, truncate=False)


# **3. Cor/raça/etnia por nível de senioridade** (escopo técnico).

# In[ ]:


resultado_p3_3 = spark.sql("""
    SELECT nivel_senioridade, cor_raca_etnia, COUNT(*) AS total
    FROM state_of_data WHERE aplica_analise_tecnica = true
    GROUP BY nivel_senioridade, cor_raca_etnia ORDER BY nivel_senioridade, total DESC
""")
resultado_p3_3.show(50, truncate=False)


# ### P4 — Tecnologias mais adotadas
# 
# **1. Linguagens mais usadas.** Bloco de múltipla escolha (escopo técnico): somo cada
# opção com `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`. Monto a query a partir da lista de
# colunas do bloco pra não repetir SQL na mão.

# In[ ]:


linguagens = [
    ("SQL", "linguagens_trabalho__sql"),
    ("R", "linguagens_trabalho__r"),
    ("Python", "linguagens_trabalho__python"),
    ("C/C++/C#", "linguagens_trabalho__c_c_c"),
    ("Julia", "linguagens_trabalho__julia"),
    ("VBA", "linguagens_trabalho__visual_basic_vba"),
    ("Scala", "linguagens_trabalho__scala"),
    ("DAX", "linguagens_trabalho__dax"),
    ("Rust", "linguagens_trabalho__rust"),
    ("Não utiliza", "linguagens_trabalho__nao_utilizo_nenhuma_das_linguagens_listadas"),
]
resultado_p4_1 = spark.sql(soma_multipla_escolha(linguagens)).withColumnRenamed("categoria", "linguagem")
resultado_p4_1.show(20, truncate=False)
grafico_barh(resultado_p4_1.toPandas(), "linguagem", "total", "Linguagens mais usadas 2025", "Total")


# **2. Cloud predominante** (mesmo padrão de múltipla escolha, escopo técnico).

# In[ ]:


clouds = [
    ("AWS", "cloud__amazon_web_services_aws"),
    ("GCP", "cloud__google_cloud_gcp"),
    ("Azure", "cloud__azure_microsoft"),
    ("Oracle Cloud", "cloud__oracle_cloud"),
    ("IBM", "cloud__ibm"),
    ("On Premise/Nenhuma", "cloud__servidores_on_premise_nao_utilizamos_cloud"),
    ("Cloud Própria", "cloud__cloud_propria"),
]
resultado_p4_2 = spark.sql(soma_multipla_escolha(clouds)).withColumnRenamed("categoria", "cloud")
resultado_p4_2.show(truncate=False)
grafico_barh(resultado_p4_2.toPandas(), "cloud", "total", "Cloud mais usada 2025", "Total")


# ### P5 — Adoção de IA
# 
# **1. IA generativa como prioridade na empresa** (categoria, ignoro nulos).

# In[ ]:


resultado_p5_1 = spark.sql("""
    SELECT ia_prioridade, COUNT(*) AS total,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentual
    FROM state_of_data WHERE ia_prioridade IS NOT NULL
    GROUP BY ia_prioridade ORDER BY total DESC
""")
resultado_p5_1.show(truncate=False)


# **2. Quem paga pela IA usada no trabalho** (bloco de uso pessoal, escopo técnico).

# In[ ]:


uso_ia = [
    ("Não usa", "ia_uso_pessoal__nao_uso_solucoes_de_ai_generativa_com_foco_em_produtividade"),
    ("Usa grátis", "ia_uso_pessoal__uso_solucoes_gratuitas_de_ai_generativa_com_foco_em_produtividade"),
    ("Usa e paga", "ia_uso_pessoal__uso_e_pago_pelas_solucoes_de_ai_generativa_com_foco_em_produtividade"),
    ("Empresa paga", "ia_uso_pessoal__a_empresa_que_trabalho_paga_pelas_solucoes_de_ai_generativa_com_foco_em_produtividade"),
    ("Usa Copilot", "ia_uso_pessoal__uso_solucoes_do_tipo_copilot"),
]
resultado_p5_2 = spark.sql(soma_multipla_escolha(uso_ia)).withColumnRenamed("categoria", "uso_pessoal")
resultado_p5_2.show(truncate=False)
grafico_barh(resultado_p5_2.toPandas(), "uso_pessoal", "total", "Uso pessoal de IA generativa 2025", "Total")


# ### P6 — Diferenças por região, senioridade e modelo de trabalho
# 
# **1. Salário por região e senioridade.** Abro por senioridade de propósito (viés de
# composição), e restrinjo aos três níveis com ordem lógica.

# In[ ]:


resultado_p6_1 = spark.sql("""
    SELECT regiao_atual, nivel_senioridade,
           ROUND(AVG(faixa_salarial_num), 0) AS salario_medio_estimado, COUNT(*) AS total
    FROM state_of_data_num
    WHERE aplica_analise_emprego = true AND aplica_analise_tecnica = true
      AND regiao_atual IS NOT NULL AND nivel_senioridade IN ('Júnior', 'Pleno', 'Sênior')
    GROUP BY regiao_atual, nivel_senioridade ORDER BY regiao_atual, nivel_senioridade
""")
resultado_p6_1.show(30, truncate=False)


# In[ ]:


import seaborn as sns
pdf = resultado_p6_1.toPandas()
paleta = {"Sênior": "#08306B", "Pleno": "#2171B5", "Júnior": "#6BAED6"}
# A query acima filtra nivel_senioridade IN ('Júnior','Pleno','Sênior') de propósito
# (pra comparar com 2023, que não tinha o nível "Especialista/Staff+"). O hue_order
# precisa refletir esse mesmo filtro -- se usar ordem_senioridade cheio (4 níveis),
# o Seaborn exige cor pra "Especialista/Staff+" também e quebra, mesmo ela não
# aparecendo nos dados.
hue_order_p6_1 = [nivel for nivel in reversed(ordem_senioridade) if nivel in paleta]
plt.figure(figsize=(11, 6))
sns.barplot(data=pdf, y="regiao_atual", x="salario_medio_estimado", hue="nivel_senioridade",
            hue_order=hue_order_p6_1, palette=paleta)
plt.title("Salário médio por região e senioridade 2025", fontsize=12, fontweight="bold", pad=15)
plt.xlabel("Salário médio estimado (R$)"); plt.ylabel("Região")
plt.grid(axis="x", linestyle="--", alpha=0.4)
plt.legend(title="Senioridade", bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)
sns.despine(); plt.tight_layout(); plt.show()


# **2. Salário por modelo de trabalho (aberto por senioridade).**

# In[ ]:


resultado_p6_2 = spark.sql("""
    SELECT nivel_senioridade, modelo_trabalho_atual,
           ROUND(AVG(faixa_salarial_num), 0) AS salario_medio_estimado, COUNT(*) AS total
    FROM state_of_data_num
    WHERE aplica_analise_emprego = true AND aplica_analise_tecnica = true
    GROUP BY nivel_senioridade, modelo_trabalho_atual
    ORDER BY nivel_senioridade, salario_medio_estimado DESC
""")
resultado_p6_2.show(30, truncate=False)


# **3. Salário por nível de ensino** (base empregada).

# In[ ]:


resultado_p6_3 = spark.sql("""
    SELECT nivel_ensino, ROUND(AVG(faixa_salarial_num), 0) AS salario_medio_estimado, COUNT(*) AS total
    FROM state_of_data_num WHERE aplica_analise_emprego = true
    GROUP BY nivel_ensino ORDER BY salario_medio_estimado DESC
""")
resultado_p6_3.show(truncate=False)
grafico_barh(resultado_p6_3.toPandas(), "nivel_ensino", "salario_medio_estimado",
             "Salário médio por nível de ensino 2025", "Salário médio estimado (R$)")


# **4. Atitude diante de um retorno presencial obrigatório** (base empregada).

# In[ ]:


import textwrap
resultado_p6_4 = spark.sql("""
    SELECT atitude_retorno_presencial, COUNT(*) AS total
    FROM state_of_data
    WHERE aplica_analise_emprego = true AND atitude_retorno_presencial IS NOT NULL
    GROUP BY atitude_retorno_presencial ORDER BY total DESC
""")
resultado_p6_4.show(truncate=False)
pdf = resultado_p6_4.toPandas()
pdf["fmt"] = pdf["atitude_retorno_presencial"].apply(lambda x: textwrap.fill(str(x), 25))
grafico_barh(pdf, "fmt", "total", "Atitude sobre retorno presencial 2025", "Quantidade de pessoas")


# ### P7 — Oportunidades e desafios
# 
# **1. Satisfação geral** (base empregada; `satisfeito_empresa` já é boolean).

# In[ ]:


resultado_p7_1 = spark.sql("""
    SELECT CASE WHEN satisfeito_empresa = true THEN 'Satisfeito'
                WHEN satisfeito_empresa = false THEN 'Insatisfeito' END AS satisfeito_empresa,
           COUNT(*) AS total,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentual
    FROM state_of_data
    WHERE aplica_analise_emprego = true AND satisfeito_empresa IS NOT NULL
    GROUP BY 1 ORDER BY total DESC
""")
resultado_p7_1.show(truncate=False)
grafico_barh(resultado_p7_1.toPandas(), "satisfeito_empresa", "total",
             "Satisfação com a empresa 2025", "Quantidade de pessoas")


# **2. Pretensão de trocar de emprego em 6 meses** (base empregada).

# In[ ]:


import textwrap
resultado_p7_2 = spark.sql("""
    SELECT pretende_mudar_emprego, COUNT(*) AS total,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentual
    FROM state_of_data
    WHERE aplica_analise_emprego = true AND pretende_mudar_emprego IS NOT NULL
    GROUP BY pretende_mudar_emprego ORDER BY total DESC
""")
resultado_p7_2.show(truncate=False)
pdf = resultado_p7_2.toPandas()
pdf["fmt"] = pdf["pretende_mudar_emprego"].apply(lambda x: textwrap.fill(str(x), 30))
grafico_barh(pdf, "fmt", "total", "Pretensão de troca de emprego 2025", "Quantidade de pessoas")


# **3. Principais desafios dos gestores.** Bloco de múltipla escolha com escopo de
# gestor — somo cada desafio a partir da lista de colunas.

# In[ ]:


desafios = [
    ("Contratar talentos", "desafios_gestor__contratar_talentos"),
    ("Reter talentos", "desafios_gestor__reter_talentos"),
    ("Convencer a empresa a investir", "desafios_gestor__convencer_a_empresa_a_aumentar_investimentos"),
    ("Gestão de equipes remotas", "desafios_gestor__gestao_de_equipes_no_ambiente_remoto"),
    ("Projetos multidisciplinares", "desafios_gestor__gestao_de_projetos_envolvendo_areas_multidisciplinares"),
    ("Qualidade/confiabilidade", "desafios_gestor__organizar_as_informacoes_com_qualidade_e_confiabilidade"),
    ("Processar/armazenar alto volume", "desafios_gestor__processar_e_armazenar_um_alto_volume_de_dados"),
    ("Gerar valor para o negócio", "desafios_gestor__gerar_valor_para_as_areas_de_negocios"),
    ("Modelos de ML em produção", "desafios_gestor__desenvolver_e_manter_modelos_machine_learning_em_producao"),
    ("Gerenciar expectativa das áreas", "desafios_gestor__gerenciar_a_expectativa_das_areas"),
    ("Manutenção de projetos/modelos", "desafios_gestor__garantir_a_manutencao_dos_projetos_e_modelos_em_producao"),
    ("Levar inovação", "desafios_gestor__conseguir_levar_inovacao_para_a_empresa"),
    ("Garantir ROI", "desafios_gestor__garantir_roi_em_projetos_de_dados"),
    ("Dividir tempo técnico/gestão", "desafios_gestor__dividir_o_tempo_entre_entregas_tecnicas_e_gestao"),
]
resultado_p7_3 = spark.sql(soma_multipla_escolha(desafios, escopo="aplica_analise_gestor")).withColumnRenamed("categoria", "desafio")
resultado_p7_3.show(truncate=False)
grafico_barh(resultado_p7_3.toPandas(), "desafio", "total",
             "Principais desafios dos gestores 2025", "Quantidade de pessoas")


# ## 4. Exportando as tabelas Gold
# 
# Persisto **uma tabela por sub-pergunta** (cada uma tem um grão/`GROUP BY` próprio),
# organizadas em 7 pastas por pergunta do desafio. Cada tabela leva a coluna
# `ano_pesquisa` e é gravada particionada — com `partitionOverwriteMode = dynamic`,
# escrevo só a partição 2025 sem apagar as outras edições no mesmo caminho.

# In[ ]:


ANO_PESQUISA = 2025

tabelas_por_pergunta = {
    "p1_estrutura_mercado": {
        "situacao_trabalho": resultado_p1_1, "setor_top10": resultado_p1_2,
        "distribuicao_cargos": resultado_p1_3, "distribuicao_senioridade": resultado_p1_4,
        "percentual_gestores": resultado_p1_5, "experiencia_x_senioridade": resultado_p1_6,
        "modelo_trabalho_atual": resultado_p1_7, "modelo_trabalho_ideal": resultado_p1_7_1,
        "porte_empresa": resultado_p1_8,
    },
    "p2_perfis_valorizados": {
        "salario_por_cargo_senioridade": resultado_p2_1, "salario_por_experiencia_dados": resultado_p2_2,
        "salario_por_experiencia_ti": resultado_p2_2_1, "salario_migracao_ti": resultado_p2_3,
        "salario_por_funcao_senioridade": resultado_p2_4, "objetivos_carreira_top5": resultado_p2_5,
    },
    "p3_diversidade": {
        "proporcao_genero": resultado_p3_1, "gap_salarial_genero": resultado_p3_2,
        "raca_por_senioridade": resultado_p3_3,
    },
    "p4_tecnologias": {
        "linguagens_mais_usadas": resultado_p4_1, "cloud_predominante": resultado_p4_2,
    },
    "p5_ia_generativa": {
        "prioridade_ia_empresa": resultado_p5_1, "quem_paga_ia": resultado_p5_2,
    },
    "p6_diferencas_regionais": {
        "salario_por_regiao_senioridade": resultado_p6_1, "salario_por_modelo_trabalho": resultado_p6_2,
        "salario_por_nivel_ensino": resultado_p6_3, "atitude_retorno_presencial": resultado_p6_4,
    },
    "p7_oportunidades_desafios": {
        "satisfacao_geral": resultado_p7_1, "intencao_troca_emprego": resultado_p7_2,
        "desafios_gestores": resultado_p7_3,
    },
}

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

for pasta, tabelas in tabelas_por_pergunta.items():
    for nome_tabela, df_resultado in tabelas.items():
        caminho = f"{CAMINHO_GOLD_BASE}/{pasta}/{nome_tabela}"
        (df_resultado.withColumn("ano_pesquisa", F.lit(ANO_PESQUISA))
         .write.mode("overwrite").partitionBy("ano_pesquisa").parquet(str(caminho)))
    print(f"{pasta}: {len(tabelas)} tabela(s) exportada(s)")

total_tabelas = sum(len(t) for t in tabelas_por_pergunta.values())
print(f"\nTotal: {total_tabelas} tabelas Gold exportadas. Partição: ano_pesquisa={ANO_PESQUISA}")


# ## 5. Conclusão
# 
# A Gold 2025 ficou com 29 tabelas nas 7 pastas por pergunta, cada uma no grão certo
# pra virar gráfico/card na apresentação executiva, e particionada por `ano_pesquisa`
# pra empilhar com as demais edições no Athena.
