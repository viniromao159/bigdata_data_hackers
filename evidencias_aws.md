# Evidências de execução na AWS

Os notebooks em `notebooks/` rodam localmente por padrão (mais simples pra
desenvolver e depurar). A execução real na nuvem — que o desafio pede
explicitamente — foi feita a partir dos scripts em [`glue_jobs/`](../glue_jobs/),
publicados como **AWS Glue Jobs**, lendo/escrevendo direto no **Amazon S3**,
catalogados no **Glue Data Catalog** e consultados via **Amazon Athena**.

Como o AWS Academy Lab usa credenciais temporárias (sessão expira em ~4h,
sem acesso persistente fora da aula), a evidência abaixo é por captura de
tela, feita no momento de cada execução — não por link ao vivo.

---

## 1. Glue Jobs — execução do pipeline

<!-- Print da aba "Runs" do Glue Studio de cada Job, mostrando status
     Succeeded, duração e timestamp. Um bloco por edição/camada rodada. -->

### Bronze

| Edição | Status | Duração | Custo estimado |
|---|---|---|---|
| 2023 | <!-- Succeeded / Failed --> | <!-- ex: 2min23s --> | <!-- ex: $0.03 --> |
| 2024 | | | |
| 2025 | | | |

`![Glue Job Bronze - execução](./screenshots/glue_bronze.png)`

### Silver

| Edição | Status | Duração | Custo estimado |
|---|---|---|---|
| 2023 | | | |
| 2024 | | | |
| 2025 | | | |

`![Glue Job Silver - execução](./screenshots/glue_silver.png)`

### Gold

| Edição | Status | Duração | Custo estimado |
|---|---|---|---|
| 2023 | | | |
| 2024 | | | |
| 2025 | | | |

`![Glue Job Gold - execução](./screenshots/glue_gold.png)`

---

## 2. Amazon S3 — Data Lake particionado

<!-- Print do console do S3 mostrando a árvore de pastas com as partições
     ano_pesquisa=2023 / 2024 / 2025 populadas em cada camada. -->

`![S3 - Bronze particionada](./screenshots/s3_bronze.png)`

`![S3 - Silver particionada](./screenshots/s3_silver.png)`

`![S3 - Gold, 7 pastas de pergunta](./screenshots/s3_gold.png)`

---

## 3. AWS Glue Data Catalog — catalogação

<!-- Print da lista de tabelas catalogadas (crawler ou catalogação manual),
     mostrando o schema reconhecido a partir do Parquet da Gold. -->

`![Glue Data Catalog - tabelas](./screenshots/glue_catalog.png)`

---

## 4. Amazon Athena — consultas analíticas

<!-- Print de uma query rodando contra as tabelas catalogadas, com
     resultado visível. Se possível, usar uma das queries comparativas
     entre as 3 edições (ex: SELECT ano_pesquisa, ... GROUP BY ano_pesquisa). -->

`![Athena - query comparativa entre edições](./screenshots/athena_query.png)`

---

## 5. Power BI — consumo final

<!-- Print do dashboard conectado via Athena, mostrando os dados das
     3 edições. -->

`![Power BI - dashboard conectado ao Athena](./screenshots/powerbi_dashboard.png)`

---

## Resumo

- Pipeline completo (Bronze → Silver → Gold) executado como **Glue Job**
  para as 3 edições, com overwrite dinâmico por partição `ano_pesquisa`.
- Dado catalogado no **Glue Data Catalog** e consultável via **Amazon Athena**.
- Consumo final em **Power BI**, conectado via Athena.
- Custo total medido ao longo dos testes: <!-- ex: menos de $1 -->.
