-- =============================================================================
-- Tech Challenge - State of Data Brasil (edição 2024/2025)
-- Cada "Pergunta de Apoio" da planilha Tech_Challenge_3_-_Grupo_6.xlsx (abas
-- P1 a P7) é respondida por UMA query logo abaixo dela, na mesma ordem/numeração
-- da planilha.
--
-- Como usar:
--   1. Spark local/Glue Notebook: registre a Silver como view antes de rodar:
--        df.createOrReplaceTempView("state_of_data_silver")
--   2. Athena: troque `state_of_data_silver` pelo nome qualificado no Glue
--      Data Catalog, ex: tech_challenge_db.state_of_data_silver
--
-- Todas as queries abaixo foram RODADAS DE VERDADE contra a Silver da edição
-- 2024_2025 (minha parte) antes de entrar aqui.
-- =============================================================================


-- #############################################################################
-- P1 — Como está estruturado o mercado brasileiro de Dados?
-- #############################################################################

-- P1.1: Como se distribui a situação de trabalho dos profissionais
-- (CLT, PJ, freelancer, desempregado, etc.)?
SELECT
    situacao_trabalho,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM state_of_data_silver
GROUP BY situacao_trabalho
ORDER BY total DESC;

-- P1.2: Em quais setores da economia esses profissionais mais atuam?
-- (top 10 setores)
SELECT
    setor,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE setor IS NOT NULL
GROUP BY setor
ORDER BY total DESC
LIMIT 10;

-- P1.3: Qual a distribuição de cargos atuais na área de dados,
-- quais são os mais comuns?
SELECT
    cargo_atual,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE cargo_atual IS NOT NULL
GROUP BY cargo_atual
ORDER BY total DESC;

-- P1.4: Qual a distribuição de senioridade no mercado
-- (% júnior, pleno, sênior)?
SELECT
    nivel,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM state_of_data_silver
WHERE nivel IS NOT NULL
GROUP BY nivel
ORDER BY total DESC;

-- P1.5: Qual % dos profissionais atua como gestor?
SELECT
    atua_como_gestor,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM state_of_data_silver
WHERE atua_como_gestor IS NOT NULL
GROUP BY atua_como_gestor;

-- P1.6: Qual é a relação entre tempo de experiência em dados
-- e nível de senioridade?
SELECT
    exp_dados,
    nivel,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE exp_dados IS NOT NULL AND nivel IS NOT NULL
GROUP BY exp_dados, nivel
ORDER BY exp_dados, total DESC;

-- P1.7: Qual modelo de trabalho predomina (remoto, híbrido, presencial)
-- e qual é o desejado?
SELECT
    'Atual' AS tipo, modelo_trabalho_atual AS modelo, COUNT(*) AS total
FROM state_of_data_silver
WHERE modelo_trabalho_atual IS NOT NULL
GROUP BY modelo_trabalho_atual
UNION ALL
SELECT
    'Ideal' AS tipo, modelo_trabalho_ideal AS modelo, COUNT(*) AS total
FROM state_of_data_silver
WHERE modelo_trabalho_ideal IS NOT NULL
GROUP BY modelo_trabalho_ideal
ORDER BY tipo, total DESC;

-- P1.8: Como se distribui o porte das empresas que possuem times de dados?
-- (o Carlos não conseguiu responder na base 2023 por falta de variável --
-- na 2024/2025 dá pra responder cruzando porte da empresa com "tem pelo
-- menos 1 cargo de dados reportado no time")
SELECT
    num_funcionarios,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE num_funcionarios IS NOT NULL
  AND (
        cargo_analytics_engineer = true OR cargo_data_engineer = true OR
        cargo_data_analyst      = true OR cargo_data_scientist = true OR
        cargo_dba               = true OR cargo_bi_analyst    = true OR
        cargo_data_architect    = true OR cargo_dpm           = true OR
        cargo_business_analyst  = true OR cargo_ml_ai_engineer = true
      )
GROUP BY num_funcionarios
ORDER BY total DESC;


-- #############################################################################
-- P2 — Quais perfis profissionais são mais valorizados pelo mercado?
-- #############################################################################

-- P2.1: Qual a faixa salarial por cargo e nível de senioridade?
SELECT
    cargo_atual,
    nivel,
    faixa_salarial,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE cargo_atual IS NOT NULL AND nivel IS NOT NULL AND faixa_salarial IS NOT NULL
GROUP BY cargo_atual, nivel, faixa_salarial
ORDER BY cargo_atual, nivel, total DESC;

-- P2.2: Profissionais com mais tempo de experiência em TI e Dados
-- são mais bem remunerados?
SELECT
    exp_dados,
    exp_ti,
    faixa_salarial,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE exp_dados IS NOT NULL AND exp_ti IS NOT NULL AND faixa_salarial IS NOT NULL
GROUP BY exp_dados, exp_ti, faixa_salarial
ORDER BY exp_dados, total DESC;

-- P2.3: Existe diferença significativa de remuneração entre profissionais
-- que migraram de TI para dados e os que iniciaram diretamente em dados?
SELECT
    CASE
        WHEN exp_ti = 'Não tive experiência na área de TI/Engenharia de Software antes de começar a trabalhar na área de dados'
            THEN 'Começou direto em Dados'
        WHEN exp_ti IS NOT NULL THEN 'Migrou de TI para Dados'
        ELSE NULL
    END AS origem_de_carreira,
    faixa_salarial,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE exp_ti IS NOT NULL AND faixa_salarial IS NOT NULL
GROUP BY
    CASE
        WHEN exp_ti = 'Não tive experiência na área de TI/Engenharia de Software antes de começar a trabalhar na área de dados'
            THEN 'Começou direto em Dados'
        WHEN exp_ti IS NOT NULL THEN 'Migrou de TI para Dados'
        ELSE NULL
    END,
    faixa_salarial
ORDER BY origem_de_carreira, total DESC;

-- P2.4: Qual é a relação entre senioridade e remuneração por função
-- (Data Engineer, Data Analytics, Data Science)?
SELECT
    funcao_atuacao,
    nivel,
    faixa_salarial,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE funcao_atuacao IS NOT NULL AND nivel IS NOT NULL AND faixa_salarial IS NOT NULL
GROUP BY funcao_atuacao, nivel, faixa_salarial
ORDER BY funcao_atuacao, nivel, total DESC;

-- P2.5: Quais são os objetivos de carreira mais citados na área de dados?
-- (o Carlos não conseguiu responder na base 2023 por falta de variável --
-- a coluna existe na 2024/2025, ~10% de resposta porque é condicionada a
-- quem está buscando recolocação/mudança de carreira)
SELECT
    objetivo_dados,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE objetivo_dados IS NOT NULL
GROUP BY objetivo_dados
ORDER BY total DESC;


-- #############################################################################
-- P3 — Qual é o cenário de diversidade de gênero nas carreiras de dados?
-- #############################################################################

-- P3.1: Qual é a proporção de gênero entre os profissionais de dados?
SELECT
    genero,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM state_of_data_silver
GROUP BY genero
ORDER BY total DESC;

-- P3.2: Existe gap salarial entre gêneros no mesmo cargo e senioridade?
SELECT
    cargo_atual,
    nivel,
    genero,
    faixa_salarial,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE cargo_atual IS NOT NULL AND nivel IS NOT NULL
  AND genero IS NOT NULL AND faixa_salarial IS NOT NULL
GROUP BY cargo_atual, nivel, genero, faixa_salarial
ORDER BY cargo_atual, nivel, genero;

-- P3.3: Como a representatividade de cor/raça/etnia se distribui
-- entre níveis de senioridade?
SELECT
    nivel,
    cor_raca_etnia,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY nivel), 1) AS pct_dentro_do_nivel
FROM state_of_data_silver
WHERE nivel IS NOT NULL AND cor_raca_etnia IS NOT NULL
GROUP BY nivel, cor_raca_etnia
ORDER BY nivel, total DESC;


-- #############################################################################
-- P4 — Quais tecnologias apresentam maior adoção entre os profissionais?
-- #############################################################################

-- P4.1: Quais linguagens de programação são mais utilizadas no dia a dia?
SELECT 'Python' AS linguagem, SUM(CAST(lang_python AS INT)) AS total FROM state_of_data_silver
UNION ALL SELECT 'SQL',        SUM(CAST(lang_sql AS INT))        FROM state_of_data_silver
UNION ALL SELECT 'R',          SUM(CAST(lang_r AS INT))          FROM state_of_data_silver
UNION ALL SELECT 'Java',       SUM(CAST(lang_java AS INT))       FROM state_of_data_silver
UNION ALL SELECT 'Scala',      SUM(CAST(lang_scala AS INT))      FROM state_of_data_silver
UNION ALL SELECT 'VBA',        SUM(CAST(lang_vba AS INT))        FROM state_of_data_silver
UNION ALL SELECT 'JavaScript',  SUM(CAST(lang_javascript AS INT)) FROM state_of_data_silver
UNION ALL SELECT 'C/C++/C#',    SUM(CAST(lang_c AS INT))         FROM state_of_data_silver
ORDER BY total DESC;

-- P4.2: Qual provedor de cloud predomina e qual é o preferido?
SELECT 'AWS (usa no dia a dia)' AS categoria, SUM(CAST(cloud_aws AS INT)) AS total FROM state_of_data_silver
UNION ALL SELECT 'GCP (usa no dia a dia)',   SUM(CAST(cloud_gcp AS INT))   FROM state_of_data_silver
UNION ALL SELECT 'Azure (usa no dia a dia)', SUM(CAST(cloud_azure AS INT)) FROM state_of_data_silver
ORDER BY total DESC;

SELECT
    cloud_preferida,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE cloud_preferida IS NOT NULL
GROUP BY cloud_preferida
ORDER BY total DESC;


-- #############################################################################
-- P5 — Qual é o índice de adoção de Inteligência Artificial e seu impacto?
-- #############################################################################

-- P5.1: Qual proporção de empresas trata IA Generativa/LLMs como prioridade?
-- (só quem é gestor responde essa pergunta na pesquisa)
SELECT
    ia_prioridade_gestor,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM state_of_data_silver
WHERE atua_como_gestor = true AND ia_prioridade_gestor IS NOT NULL
GROUP BY ia_prioridade_gestor
ORDER BY total DESC;

-- P5.2: As empresas estão conseguindo bons resultados com LLMs?
-- NÃO RESPONDIDA -- coluna não existe nem na base 2023 (Carlos) nem na 2024/2025.
-- Conferi manualmente no header do CSV: não há nenhuma pergunta sobre
-- "resultado"/"sucesso" do uso de LLM nesta edição. Precisa confirmar se
-- existe na base 2025/2026 do Vini antes de decidir se dá pra responder
-- com pelo menos uma das 3 edições.

-- P5.3: Quem paga pela IA usada no trabalho, o profissional ou a empresa?
SELECT 'Não uso IA generativa'          AS quem_paga, SUM(CAST(ia_pessoal_nao_usa AS INT))       AS total FROM state_of_data_silver
UNION ALL SELECT 'Uso solução gratuita', SUM(CAST(ia_pessoal_gratuita AS INT))     FROM state_of_data_silver
UNION ALL SELECT 'Eu mesmo pago',        SUM(CAST(ia_pessoal_paga AS INT))         FROM state_of_data_silver
UNION ALL SELECT 'A empresa paga',       SUM(CAST(ia_pessoal_empresa_paga AS INT)) FROM state_of_data_silver
UNION ALL SELECT 'Uso tipo Copilot (via empresa/ferramenta)', SUM(CAST(ia_pessoal_copilot AS INT)) FROM state_of_data_silver
ORDER BY total DESC;


-- #############################################################################
-- P6 — Existem diferenças relevantes entre regiões, senioridades
--      ou modelos de trabalho?
-- #############################################################################

-- P6.1: Como a faixa salarial varia entre regiões do Brasil?
SELECT
    regiao,
    faixa_salarial,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE regiao IS NOT NULL AND faixa_salarial IS NOT NULL
GROUP BY regiao, faixa_salarial
ORDER BY regiao, total DESC;

-- P6.2: Existe diferença salarial entre modelos de trabalho
-- (remoto vs presencial vs híbrido)?
SELECT
    modelo_trabalho_atual,
    faixa_salarial,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE modelo_trabalho_atual IS NOT NULL AND faixa_salarial IS NOT NULL
GROUP BY modelo_trabalho_atual, faixa_salarial
ORDER BY modelo_trabalho_atual, total DESC;

-- P6.3: Qual é a correlação entre nível de ensino/área de formação
-- e faixa salarial?
SELECT
    nivel_ensino,
    area_formacao,
    faixa_salarial,
    COUNT(*) AS total
FROM state_of_data_silver
WHERE nivel_ensino IS NOT NULL AND faixa_salarial IS NOT NULL
GROUP BY nivel_ensino, area_formacao, faixa_salarial
ORDER BY nivel_ensino, total DESC;

-- P6.4: Qual seria a atitude dos profissionais diante de um retorno
-- presencial obrigatório?
SELECT
    atitude_retorno_presencial,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM state_of_data_silver
WHERE atitude_retorno_presencial IS NOT NULL
GROUP BY atitude_retorno_presencial
ORDER BY total DESC;


-- #############################################################################
-- P7 — Quais oportunidades e desafios podem ser identificados para empresas
--      que desejam investir em Dados e IA?
-- #############################################################################

-- P7.1: Qual é o nível de satisfação geral dos profissionais de dados?
SELECT
    satisfeito,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM state_of_data_silver
WHERE satisfeito IS NOT NULL
GROUP BY satisfeito;

-- P7.2: Qual proporção planeja mudar de emprego nos próximos 6 meses?
SELECT
    planos_mudar_6m,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM state_of_data_silver
WHERE planos_mudar_6m IS NOT NULL
GROUP BY planos_mudar_6m
ORDER BY total DESC;

-- P7.3: Quais são os desafios mais citados por gestores de dados?
SELECT 'Contratar talentos' AS desafio, SUM(CAST(desafio_contratar AS INT)) AS total FROM state_of_data_silver
UNION ALL SELECT 'Reter talentos',              SUM(CAST(desafio_reter AS INT))              FROM state_of_data_silver
UNION ALL SELECT 'Convencer investimento',      SUM(CAST(desafio_investimento AS INT))       FROM state_of_data_silver
UNION ALL SELECT 'Gestão de equipes remotas',   SUM(CAST(desafio_remoto AS INT))             FROM state_of_data_silver
UNION ALL SELECT 'Projetos multidisciplinares', SUM(CAST(desafio_multidisciplinar AS INT))   FROM state_of_data_silver
UNION ALL SELECT 'Qualidade da informação',     SUM(CAST(desafio_qualidade_info AS INT))     FROM state_of_data_silver
UNION ALL SELECT 'Alto volume de dados',        SUM(CAST(desafio_volume AS INT))             FROM state_of_data_silver
UNION ALL SELECT 'Gerar valor pro negócio',     SUM(CAST(desafio_gerar_valor AS INT))        FROM state_of_data_silver
UNION ALL SELECT 'ML em produção',              SUM(CAST(desafio_ml_producao AS INT))        FROM state_of_data_silver
UNION ALL SELECT 'Gerenciar expectativas',      SUM(CAST(desafio_expectativa AS INT))        FROM state_of_data_silver
UNION ALL SELECT 'Manutenção em produção',      SUM(CAST(desafio_manutencao AS INT))         FROM state_of_data_silver
UNION ALL SELECT 'Levar inovação',              SUM(CAST(desafio_inovacao AS INT))           FROM state_of_data_silver
UNION ALL SELECT 'Garantir ROI',                SUM(CAST(desafio_roi AS INT))                FROM state_of_data_silver
UNION ALL SELECT 'Dividir tempo técnico/gestão',SUM(CAST(desafio_tempo_tecnico_gestao AS INT)) FROM state_of_data_silver
ORDER BY total DESC;
