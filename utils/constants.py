"""Constants Carlos"""

"""
Constantes compartilhadas entre as camadas Silver e Gold.

Consolidado a partir do que o notebook gold_2023_2024 vinha deixando
hardcoded inline (CASE WHEN em SQL, listas Python soltas em células de
gráfico). Import sugerido:

    from utils.constants import ordem_senioridade, ordem_salarial, ponto_medio_salarial
"""

# --------------------------------------------------------------------------
# Ordem lógica de senioridade (Júnior -> Pleno -> Sênior), usada em gráficos
# e ORDER BY que não podem confiar em ordem alfabética.
# --------------------------------------------------------------------------

ordem_senioridade = ["Júnior", "Pleno", "Sênior"]


# --------------------------------------------------------------------------
# Ponto médio (R$) de cada faixa salarial, usado pra calcular médias e
# comparar grupos (ex: salário médio por região, por cargo, por gênero).
# A ordem do dict já é a ordem lógica das faixas (da mais baixa pra mais alta).
# --------------------------------------------------------------------------

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

# Lista de faixas na ordem lógica (mais baixa -> mais alta), pra usar em
# ORDER BY CASE faixa_salarial WHEN ... ou em eixos de gráfico ordenados.
ordem_salarial = list(ponto_medio_salarial.keys())


# --------------------------------------------------------------------------
# Ordem lógica de tempo de experiência, usada no ORDER BY CASE das queries
# de P2.2 (tempo_experiencia_dados / tempo_experiencia_ti) na Gold.
# Os dois textos de "sem experiência" são tratados como equivalentes (0).
# --------------------------------------------------------------------------

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



"""Constants Maycon"""

"""
Constantes compartilhadas do projeto Tech Challenge - State of Data Brasil.

Cada dono de edição (Carlos=2023_2024, Maycon=2024_2025, Vini=2025_2026)
usa a mesma estrutura de path e o mesmo padrão de nomes tratados -- só o
dicionário de colunas muda por edição.
"""

# =============================================================================
# EDIÇÃO DESTA BASE (usada como valor da coluna "edicao" na Silver)
# =============================================================================
EDICAO = "2024_2025"

# =============================================================================
# CAMINHOS
# Local (testes na sua máquina) x AWS (S3, depois de subir pro Glue).
# Troque LOCAL_MODE para False quando o notebook for rodar no Glue.
# =============================================================================
LOCAL_MODE = True

if LOCAL_MODE:
    BASE_PATH = "../../data"
else:
    BASE_PATH = "s3://tech-challenge-state-of-data"  # ajustar para o bucket real do grupo

RAW_PATH = f"{BASE_PATH}/raw/state_of_data_{EDICAO}.csv"
BRONZE_PATH = f"{BASE_PATH}/bronze/state_of_data_{EDICAO}.parquet"
SILVER_PATH = f"{BASE_PATH}/silver/state_of_data_{EDICAO}_silver.parquet"
# a Silver unificada (03_uniao_silver.ipynb) lê as 3 SILVER_PATH de cada edição
# e escreve em f"{BASE_PATH}/silver/state_of_data_silver.parquet"

GLUE_DATABASE = "tech_challenge_db"

# =============================================================================
# DICIONÁRIO DE COLUNAS: original -> tratada (só as 174 colunas relevantes)
# Já validado 1:1 contra o header real do state_of_data_2024_2025.csv.
# =============================================================================
p1_estrutura_mercado = {
    "0.a_token": "token",
    "2.a_situação_de_trabalho": "situacao_trabalho",
    "2.b_setor": "setor",
    "2.c_numero_de_funcionarios": "num_funcionarios",
    "2.d_atua_como_gestor": "atua_como_gestor",
    "2.f_cargo_atual": "cargo_atual",
    "2.g_nivel": "nivel",
    "2.h_faixa_salarial": "faixa_salarial",
    "2.i_tempo_de_experiencia_em_dados": "exp_dados",
    "2.j_tempo_de_experiencia_em_ti": "exp_ti",
    "2.r_modelo_de_trabalho_atual": "modelo_trabalho_atual",
    "2.s_modelo_de_trabalho_ideal": "modelo_trabalho_ideal",
    "2.q_empresa_passou_por_layoff_em_2024": "layoff_ano",
    "3.a_numero_de_pessoas_em_dados": "num_pessoas_dados",
    "3.b.1_Analytics Engineer": "cargo_analytics_engineer",
    "3.b.2_Engenharia de Dados/Data Engineer": "cargo_data_engineer",
    "3.b.3_Analista de Dados/Data Analyst": "cargo_data_analyst",
    "3.b.4_Cientista de Dados/Data Scientist": "cargo_data_scientist",
    "3.b.5_Database Administrator/DBA": "cargo_dba",
    "3.b.6_Analista de Business Intelligence/BI": "cargo_bi_analyst",
    "3.b.7_Arquiteto de Dados/Data Architect": "cargo_data_architect",
    "3.b.8_Data Product Manager/DPM": "cargo_dpm",
    "3.b.9_Business Analyst": "cargo_business_analyst",
    "3.b.10_ML Engineer/AI Engineer": "cargo_ml_ai_engineer",
    "4.a.1_atuacao_em_dados": "atuacao_dados",
    "4.a_funcao_de_atuacao": "funcao_atuacao",
}

p2_perfis_valorizados = {
    "0.a_token": "token",
    "2.f_cargo_atual": "cargo_atual",
    "2.g_nivel": "nivel",
    "2.h_faixa_salarial": "faixa_salarial",
    "2.i_tempo_de_experiencia_em_dados": "exp_dados",
    "4.a.1_atuacao_em_dados": "atuacao_dados",
    "4.a_funcao_de_atuacao": "funcao_atuacao",
    "5.a_objetivo_na_area_de_dados": "objetivo_dados",
    "5.b_oportunidade_buscada": "oportunidade_buscada",
    "5.c_tempo_em_busca_de_oportunidade": "tempo_busca_oportunidade",
    "5.d_experiencia_em_processos_seletivos": "exp_processos_seletivos",
}

p3_diversidade = {
    "0.a_token": "token",
    "1.a.1_faixa_idade": "faixa_idade",
    "1.b_genero": "genero",
    "1.c_cor/raca/etnia": "cor_raca_etnia",
    "1.d_pcd": "pcd",
    "1.e_experiencia_profissional_prejudicada": "exp_prejudicada",
    "1.e.1_Não acredito que minha experiência profissional seja afetada": "prej_nao_afetada",
    "1.e.2_Sim, devido a minha Cor/Raça/Etnia": "prej_cor_raca",
    "1.e.3_Sim, devido a minha identidade de gênero": "prej_genero",
    "1.e.4_Sim, devido ao fato de ser PCD": "prej_pcd",
    "1.f.1_Quantidade de oportunidades de emprego/vagas recebidas": "prej_oportunidades_vagas",
    "1.f.2_Senioridade das vagas recebidas em relação à sua experiência": "prej_senioridade_vagas",
    "1.f.3_Aprovação em processos seletivos/entrevistas": "prej_aprovacao_seletivos",
    "1.f.4_Oportunidades de progressão de carreira": "prej_progressao",
    "1.f.5_Velocidade de progressão de carreira": "prej_velocidade_progressao",
    "1.f.6_Nível de cobrança no trabalho/Stress no trabalho": "prej_stress",
    "1.f.7_Atenção dada pelas pessoas diante das minhas opiniões e ideias": "prej_atencao_opinioes",
    "1.f.8_Relação com outras pessoas da empresa, em momentos de trabalho": "prej_relacao_trabalho",
    "1.f.9_Relação com outras pessoas da empresa, em momentos de integração e outros momentos fora do trabalho": "prej_relacao_integracao",
    "2.f_cargo_atual": "cargo_atual",
    "2.g_nivel": "nivel",
    "2.h_faixa_salarial": "faixa_salarial",
}

p4_tecnologias = {
    "0.a_token": "token",
    "4.d.1_SQL": "lang_sql",
    "4.d.2_R": "lang_r",
    "4.d.3_Python": "lang_python",
    "4.d.4_C/C++/C#": "lang_c",
    "4.d.5_.NET": "lang_dotnet",
    "4.d.6_Java": "lang_java",
    "4.d.7_Julia": "lang_julia",
    "4.d.8_SAS/Stata": "lang_sas_stata",
    "4.d.9_Visual Basic/VBA": "lang_vba",
    "4.d.10_Scala": "lang_scala",
    "4.d.11_Matlab": "lang_matlab",
    "4.d.12_Rust": "lang_rust",
    "4.d.13_PHP": "lang_php",
    "4.d.14_JavaScript": "lang_javascript",
    "4.d.15_Não utilizo nenhuma das linguagens listadas": "lang_nenhuma",
    "4.f_linguagem_preferida": "lang_preferida",
    "4.g.1_MySQL": "db_mysql",
    "4.g.2_Oracle": "db_oracle",
    "4.g.3_SQL SERVER": "db_sqlserver",
    "4.g.4_Amazon Aurora ou RDS": "db_aurora_rds",
    "4.g.5_DynamoDB": "db_dynamodb",
    "4.g.8_MongoDB": "db_mongodb",
    "4.g.11_S3": "db_s3",
    "4.g.12_PostgreSQL": "db_postgresql",
    "4.g.13_ElasticSearch": "db_elasticsearch",
    "4.g.16_SQLite": "db_sqlite",
    "4.g.20_Redis": "db_redis",
    "4.g.22_Google BigQuery": "db_bigquery",
    "4.g.24_Amazon Redshift": "db_redshift",
    "4.g.25_Amazon Athena": "db_athena",
    "4.g.26_Snowflake": "db_snowflake",
    "4.g.27_Databricks": "db_databricks",
    "4.h.1_Amazon Web Services (AWS)": "cloud_aws",
    "4.h.2_Google Cloud (GCP)": "cloud_gcp",
    "4.h.3_Azure (Microsoft)": "cloud_azure",
    "4.h.4_Oracle Cloud": "cloud_oracle",
    "4.h.5_IBM": "cloud_ibm",
    "4.h.6_Servidores On Premise/Não utilizamos Cloud": "cloud_onpremise",
    "4.h.7_Cloud Própria": "cloud_propria",
    "4.i_cloud_preferida": "cloud_preferida",
    "4.j.1_Microsoft PowerBI": "bi_powerbi",
    "4.j.2_Qlik View/Qlik Sense": "bi_qlik",
    "4.j.3_Tableau": "bi_tableau",
    "4.j.4_Metabase": "bi_metabase",
    "4.j.5_Superset": "bi_superset",
    "4.j.7_Looker": "bi_looker",
    "4.j.8_Looker Studio(Google Data Studio)": "bi_looker_studio",
    "4.j.15_Grafana": "bi_grafana",
    "4.j.17_Fazemos todas as análises utilizando apenas Excel ou planilhas do google": "bi_somente_excel",
    "4.j.18_Não utilizo nenhuma ferramenta de BI no trabalho": "bi_nenhuma",
    "4.k_ferramenta_de_bi_preferida": "bi_preferida",
    "6.c_possui_data_lake": "possui_data_lake",
    "6.d_tecnologia_data_lake": "tech_data_lake",
    "6.e_possui_data_warehouse": "possui_data_warehouse",
    "6.f_tecnologia_data_warehouse": "tech_data_warehouse",
    "6.g_ferramentas_de_qualidade_de_dados_(dia_a_dia)": "ferramentas_qualidade_dados",
    "6.b.1_Scripts Python": "etl_de_python",
    "6.b.2_SQL & Stored Procedures": "etl_de_sql",
    "6.b.3_Apache Airflow": "etl_de_airflow",
    "6.b.6_AWS Glue": "etl_de_aws_glue",
    "6.b.20_Databricks": "etl_de_databricks",
}

p5_ia_generativa = {
    "0.a_token": "token",
    "3.e_ai_generativa_e_llm_é_uma_prioridade?": "ia_prioridade_gestor",
    "3.f.1 Colaboradores usando AI generativa de forma independente e descentralizada": "ia_uso_gestor_descentralizado",
    "3.f.2 Direcionamento centralizado do uso de AI generativa": "ia_uso_gestor_centralizado",
    "3.f.3 Desenvolvedores utilizando Copilots": "ia_uso_gestor_copilots",
    "3.f.4 AI Generativa e LLMs para melhorar produtos externos para os clientes finais": "ia_uso_gestor_prod_externo",
    "3.f.5 AI Generativa e LLMs para melhorar produtos internos para os colaboradores": "ia_uso_gestor_prod_interno",
    "3.f.6 IA Generativa e LLMs como principal frente do negócio": "ia_uso_gestor_frente_negocio",
    "3.f.7 IA Generativa e LLMs não é prioridade": "ia_uso_gestor_nao_prioridade",
    "3.f.8 Não sei opinar sobre o uso de IA Generativa e LLMs na empresa": "ia_uso_gestor_nao_sabe",
    "3.g.1 Falta de compreensão dos casos de uso": "ia_barreira_compreensao",
    "3.g.2 Falta de confiabilidade das saídas (alucinação dos modelos)": "ia_barreira_alucinacao",
    "3.g.3 Incerteza em relação a regulamentação": "ia_barreira_regulamentacao",
    "3.g.4 Preocupações com segurança e privacidade de dados": "ia_barreira_seguranca",
    "3.g.5 Retorno sobre investimento (ROI) não comprovado de IA Generativa": "ia_barreira_roi",
    "3.g.6 Dados da empresa não estão prontos para uso de IA Generativa": "ia_barreira_dados",
    "3.g.7 Falta de expertise ou falta de recursos": "ia_barreira_expertise",
    "3.g.8 Alta direção da empresa não vê valor ou não vê como prioridade": "ia_barreira_direcao",
    "3.g.9 Preocupações com propriedade intelectual": "ia_barreira_ip",
    "4.l.1 Colaboradores usando AI generativa de forma independente e descentralizada": "ia_uso_ic_descentralizado",
    "4.l.2 Direcionamento centralizado do uso de AI generativa": "ia_uso_ic_centralizado",
    "4.l.3 Desenvolvedores utilizando Copilots": "ia_uso_ic_copilots",
    "4.l.4 AI Generativa e LLMs para melhorar produtos externos para os clientes finais": "ia_uso_ic_prod_externo",
    "4.l.5 AI Generativa e LLMs para melhorar produtos internos para os colaboradores": "ia_uso_ic_prod_interno",
    "4.l.6 IA Generativa e LLMs como principal frente do negócio": "ia_uso_ic_frente_negocio",
    "4.l.7 IA Generativa e LLMs não é prioridade": "ia_uso_ic_nao_prioridade",
    "4.l.8 Não sei opinar sobre o uso de IA Generativa e LLMs na empresa": "ia_uso_ic_nao_sabe",
    "4.m.1 Não uso soluções de AI Generativa com foco em produtividade": "ia_pessoal_nao_usa",
    "4.m.2 Uso soluções gratuitas de AI Generativa com foco em produtividade": "ia_pessoal_gratuita",
    "4.m.3 Uso e pago pelas soluções de AI Generativa com foco em produtividade": "ia_pessoal_paga",
    "4.m.4 A empresa que trabalho paga pelas soluções de AI Generativa com foco em produtividade": "ia_pessoal_empresa_paga",
    "4.m.5 Uso soluções do tipo Copilot": "ia_pessoal_copilot",
}

p6_diferencas_regionais = {
    "0.a_token": "token",
    "1.i.1_uf_onde_mora": "uf",
    "1.i.2_regiao_onde_mora": "regiao",
    "1.l_nivel_de_ensino": "nivel_ensino",
    "1.m_área_de_formação": "area_formacao",
    "2.g_nivel": "nivel",
    "2.h_faixa_salarial": "faixa_salarial",
    "2.r_modelo_de_trabalho_atual": "modelo_trabalho_atual",
    "2.s_modelo_de_trabalho_ideal": "modelo_trabalho_ideal",
    "2.t_atitude_em_caso_de_retorno_presencial": "atitude_retorno_presencial",
}

p7_oportunidades_desafios = {
    "0.a_token": "token",
    "2.k_satisfeito_atualmente": "satisfeito",
    "2.l_motivo_insatisfacao": "motivo_insatisfacao",
    "2.l.1_Remuneração/Salário": "insat_remuneracao",
    "2.l.2_Benefícios": "insat_beneficios",
    "2.l.3_Propósito do trabalho e da empresa": "insat_proposito",
    "2.l.4_Flexibilidade de trabalho remoto": "insat_flexibilidade",
    "2.l.5_Ambiente e clima de trabalho": "insat_ambiente",
    "2.l.6_Oportunidade de aprendizado e trabalhar com referências": "insat_aprendizado",
    "2.l.7_Oportunidades de crescimento": "insat_crescimento",
    "2.l.8_Maturidade da empresa em termos de tecnologia e dados": "insat_maturidade_empresa",
    "2.l.9_Relação com os gestores e líderes": "insat_relacao_gestores",
    "2.l.10_Reputação que a empresa tem no mercado": "insat_reputacao",
    "2.l.11_Gostaria de trabalhar em outra área": "insat_outra_area",
    "2.m_participou_de_entrevistas_ultimos_6m": "entrevistas_6m",
    "2.n_planos_de_mudar_de_emprego_6m": "planos_mudar_6m",
    "2.q_empresa_passou_por_layoff_em_2024": "layoff_ano",
    "3.d.1_Contratar talentos": "desafio_contratar",
    "3.d.2_Reter talentos": "desafio_reter",
    "3.d.3_Convencer a empresa a aumentar investimentos": "desafio_investimento",
    "3.d.4_Gestão de equipes no ambiente remoto": "desafio_remoto",
    "3.d.5_Gestão de projetos envolvendo áreas multidisciplinares": "desafio_multidisciplinar",
    "3.d.6_Organizar as informações com qualidade e confiabilidade": "desafio_qualidade_info",
    "3.d.7_Processar e armazenar um alto volume de dados": "desafio_volume",
    "3.d.8_Gerar valor para as áreas de negócios": "desafio_gerar_valor",
    "3.d.9_Desenvolver e manter modelos Machine Learning em produção": "desafio_ml_producao",
    "3.d.10_Gerenciar a expectativa das áreas": "desafio_expectativa",
    "3.d.11_Garantir a manutenção dos projetos e modelos em produção": "desafio_manutencao",
    "3.d.12_Conseguir levar inovação para a empresa": "desafio_inovacao",
    "3.d.13_Garantir (ROI) em projetos de dados": "desafio_roi",
    "3.d.14_Dividir o tempo entre entregas técnicas e gestão": "desafio_tempo_tecnico_gestao",
    "6.c_possui_data_lake": "possui_data_lake",
    "6.e_possui_data_warehouse": "possui_data_warehouse",
}

RENAME_COLUNAS = {}
for _d in [
    p1_estrutura_mercado, p2_perfis_valorizados, p3_diversidade,
    p4_tecnologias, p5_ia_generativa, p6_diferencas_regionais,
    p7_oportunidades_desafios,
]:
    RENAME_COLUNAS.update(_d)

# =============================================================================
# COLUNAS BOOLEANAS
# Já vêm True/False/NaN direto do CSV nesta edição (2024/2025) -- só reforçamos
# o tipo no schema. NaN aqui É legítimo: é a "situação de trabalho ativa" que
# faz essas perguntas nem aparecerem pra quem está desempregado/estudante.
# =============================================================================
COLUNAS_JA_BOOLEANAS = ["atua_como_gestor", "satisfeito"]

# =============================================================================
# COLUNAS DE MÚLTIPLA ESCOLHA (0.0/1.0/NaN -> boolean)
# NaN = pergunta nunca apareceu pra essa pessoa (lógica de bloco condicional).
# 0.0 = pergunta apareceu, não foi marcada. 1.0 = marcada.
# =============================================================================
COLUNAS_MULTIPLA_ESCOLHA = [v for k, v in RENAME_COLUNAS.items() if any(
    k.startswith(prefix) for prefix in (
        "3.b.", "4.d.", "4.g.", "4.h.", "4.j.", "6.b.",
        "3.f.", "3.g.", "4.l.", "4.m.", "3.d.", "2.l.", "1.e.", "1.f.",
    )
)]

# =============================================================================
# COLUNAS CUJO NULO É ESTRUTURAL (lógica de "pergunta-portão"), NÃO tratamos
# como "Sem resposta" -- documentamos aqui o motivo de cada uma.
# =============================================================================
REGRAS_DE_NULO = {
    # colunas só respondidas por quem atua_como_gestor = True
    "gestor": [
        "ia_prioridade_gestor", "ia_uso_gestor_descentralizado", "ia_uso_gestor_centralizado",
        "ia_uso_gestor_copilots", "ia_uso_gestor_prod_externo", "ia_uso_gestor_prod_interno",
        "ia_uso_gestor_frente_negocio", "ia_uso_gestor_nao_prioridade", "ia_uso_gestor_nao_sabe",
        "ia_barreira_compreensao", "ia_barreira_alucinacao", "ia_barreira_regulamentacao",
        "ia_barreira_seguranca", "ia_barreira_roi", "ia_barreira_dados", "ia_barreira_expertise",
        "ia_barreira_direcao", "ia_barreira_ip",
        "desafio_contratar", "desafio_reter", "desafio_investimento", "desafio_remoto",
        "desafio_multidisciplinar", "desafio_qualidade_info", "desafio_volume", "desafio_gerar_valor",
        "desafio_ml_producao", "desafio_expectativa", "desafio_manutencao", "desafio_inovacao",
        "desafio_roi", "desafio_tempo_tecnico_gestao",
    ],
    # colunas só respondidas por quem NÃO é gestor (contribuidor individual)
    "nao_gestor": [
        "ia_uso_ic_descentralizado", "ia_uso_ic_centralizado", "ia_uso_ic_copilots",
        "ia_uso_ic_prod_externo", "ia_uso_ic_prod_interno", "ia_uso_ic_frente_negocio",
        "ia_uso_ic_nao_prioridade", "ia_uso_ic_nao_sabe",
    ],
    # colunas do bloco P2 (~90% nulo) -- claramente condicionadas a uma
    # pergunta de filtro anterior (ex: só quem está buscando recolocação
    # ou mudança de carreira). O gate exato não é uma coluna 1:1 óbvia no
    # dicionário, então documentamos aqui em vez de preencher "Sem resposta".
    "objetivo_carreira": [
        "objetivo_dados", "oportunidade_buscada",
        "tempo_busca_oportunidade", "exp_processos_seletivos",
    ],
    # colunas só respondidas por quem tem situação de trabalho ativa
    "situacao_trabalho_ativa": [
        "cargo_atual", "nivel", "faixa_salarial", "modelo_trabalho_atual",
        "modelo_trabalho_ideal", "atitude_retorno_presencial", "satisfeito",
        "motivo_insatisfacao", "entrevistas_6m", "planos_mudar_6m",
    ],
}
