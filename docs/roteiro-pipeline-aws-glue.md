# Roteiro: replicando o pipeline State of Data Brasil no seu AWS Academy Lab

Este roteiro assume que você tem **uma conta AWS Academy Lab própria, do zero**
(diferente da conta que já rodou o pipeline). Os scripts (bronze/silver/gold,
3 edições cada = 9 arquivos `.py`) já estão prontos e testados — o que muda de
conta pra conta é só o **bucket S3** e a **role** usados.

> **O que este roteiro cobre:** criar a infraestrutura na sua conta, rodar os
> 9 jobs Glue na ordem certa, catalogar a Gold com Glue Crawler, consultar no
> Athena e conectar no Power BI via ODBC.

---

## 0. Visão geral da arquitetura

Pipeline em 3 camadas (arquitetura medalhão), rodando em **AWS Glue** (Spark
gerenciado), com dado em **Parquet** no S3:

```
data/raw/                                  <- CSV bruto de cada edição (upload manual)
data/bronze/state_of_data/                 <- Parquet, particionado por ano_pesquisa
data/bronze/metadados/mapa_colunas_*.csv   <- de-para código→nome de coluna
data/silver/state_of_data_silver/          <- Parquet, particionado por ano_pesquisa
data/silver/metadados/dict_columns_*.py    <- dicionário de colunas por pergunta
data/gold/{7 pastas}/{29 tabelas}/         <- Parquet, particionado por ano_pesquisa
```

Cada camada tem **3 scripts** (um por edição: 2023, 2024, 2025) porque cada
edição da pesquisa tem um schema de colunas diferente. Bronze/Silver/Gold
sempre leem e escrevem no **mesmo caminho compartilhado**, particionado por
`ano_pesquisa` — é assim que dá pra comparar as 3 edições juntas depois.

**Ordem de execução obrigatória, por edição:** Bronze → Silver → Gold. Entre
edições diferentes não tem dependência (pode rodar 2024 e 2025 em qualquer
ordem entre si).

---

## 1. Pré-requisitos

- [ ] Sessão do AWS Academy Lab **iniciada** (o botão "Start Lab" precisa estar
      verde/ativo — as credenciais expiram quando a sessão encerra).
- [ ] Acesso ao console AWS pelo link do Lab (não use o console AWS normal — as
      credenciais são só as da sessão do Lab).
- [ ] Os 3 arquivos CSV brutos baixados localmente (Kaggle — State of Data
      Brasil): `state_of_data_2023_2024.csv`, `state_of_data_2024_2025.csv`,
      `state_of_data_2025_2026.csv`.
- [ ] Os 9 scripts `.py` prontos (bronze/silver/gold × 2023/2024/2025).

---

## 2. Criando o bucket S3

1. No console, vá em **S3 → Create bucket**.
2. Nome do bucket: **precisa ser globalmente único** — não dá pra reusar
   `state-of-data-2023-1819-2244-3791` (esse já existe em outra conta). Sugestão:
   incluir o seu Account ID, tipo `state-of-data-<seu-account-id>`. Você acha o
   Account ID no canto superior direito do console (12 dígitos).
3. Região: a mesma que você vai usar no Glue (confira a região ativa no canto
   superior direito — o AWS Academy geralmente fixa `us-east-1`).
4. Mantenha o resto padrão (bloqueio de acesso público ligado — não precisa de
   acesso público pra esse projeto).
5. Depois de criado, crie a estrutura de pastas (pode ser vazio, ou já subir o
   primeiro arquivo direto na pasta certa — S3 cria a "pasta" a partir do
   caminho do arquivo):
   ```
   data/raw/
   data/bronze/state_of_data/
   data/bronze/metadados/
   data/silver/state_of_data_silver/
   data/silver/metadados/
   data/gold/
   ```

## 3. Upload dos CSVs brutos

Em **S3 → seu bucket → data/raw/**, faça upload dos 3 arquivos:
- `state_of_data_2023_2024.csv`
- `state_of_data_2024_2025.csv`
- `state_of_data_2025_2026.csv`

## 4. Confirmando a IAM Role

No AWS Academy Lab, toda conta de estudante já vem com uma role pronta chamada
**`LabRole`** (com as permissões necessárias pra Glue/S3 já anexadas — você não
precisa criar nada). Confirme que ela existe:

1. **IAM → Roles**, procure `LabRole`.
2. Copie o ARN completo — algo como
   `arn:aws:iam::<SEU-ACCOUNT-ID>:role/LabRole`. Você vai usar esse ARN (com o
   SEU Account ID) na configuração de cada job Glue.

Se por algum motivo `LabRole` não existir na sua conta, isso é uma
particularidade da configuração do seu Lab — não é algo que dê pra contornar
editando os scripts; vale confirmar com quem administra o Lab de vocês.

## 5. Ajustando os scripts pro seu bucket

Em **cada um dos 9 scripts**, tem uma linha no topo assim:

```python
BUCKET = "s3://state-of-data-2023-1819-2244-3791"
```

Troque pelo nome do bucket que você criou no passo 2:

```python
BUCKET = "s3://state-of-data-<seu-account-id>"
```

Essa é a **única edição de código necessária** pra rodar na sua conta — toda a
lógica de negócio (regras de escopo, parsing de coluna, mapeamento de
pergunta) já está pronta e não muda entre contas.

> Dica: como são 9 arquivos, um `find/replace` no editor de texto local (antes
> de subir pro Glue Studio) é mais rápido do que editar um por um na tela do
> Glue.

## 6. Criando os jobs no Glue Studio

Repita esse processo **9 vezes** (uma por script). No console:

1. **AWS Glue → Glue Studio → Jobs → Script editor** (não use o modo visual —
   os scripts já são Python/PySpark prontos).
2. Escolha **Spark**, engine **Python**, e cole o conteúdo do `.py`
   correspondente no editor.
3. Aba **Job details**, configure:

   | Campo | Valor |
   | :-- | :-- |
   | Name | ex: `bronze-2023`, `silver-2023`, `gold-2023`, ... |
   | IAM Role | o ARN do `LabRole` da sua conta (passo 4) |
   | Glue version | **5.1** |
   | Language | Python 3 |
   | Worker type | **G.1X** |
   | Number of workers | **2** |
   | Job bookmark | **Disable** |
   | Number of retries | 0 |
   | Job timeout (minutes) | 480 |

4. Salve o job (**Save**). Não precisa rodar ainda.

Repita para os 9 scripts. Nomeie de forma que dê pra saber a ordem de
execução — ex: prefixo `01-bronze-`, `02-silver-`, `03-gold-`.

## 7. Ordem de execução

Rode **na ordem abaixo**, esperando cada job terminar (status **Succeeded** na
aba **Runs**) antes de rodar o próximo da mesma edição:

```
1. bronze_2023  ─┐
2. silver_2023   ├─ edição 2023 (pode rodar em paralelo com as outras edições)
3. gold_2023    ─┘

4. bronze_2024  ─┐
5. silver_2024   ├─ edição 2024
6. gold_2024    ─┘

7. bronze_2025  ─┐
8. silver_2025   ├─ edição 2025
9. gold_2025    ─┘
```

Dentro de uma mesma edição, a ordem Bronze → Silver → Gold é **obrigatória**
(cada camada lê o que a anterior escreveu). Entre edições diferentes, pode
rodar em paralelo se quiser.

## 8. Validação depois de cada camada

Confira no **CloudWatch Logs** do job (link direto na aba **Runs** do job no
Glue Studio) os `print()` que os scripts já emitem:

- **Bronze:** `"X linhas, Y colunas"` (X e Y devem bater com o CSV bruto: 2023
  tem 399 colunas originais, 2024 e 2025 têm outros números — confira contra o
  print da célula de leitura do CSV) e `"Mapa de colunas salvo em: ..."`.
- **Silver:** `"OK: todos os nomes do mapa de colunas existem no schema da
  Bronze lida"` (se isso não aparecer e vier um `ValueError` no lugar, é sinal
  de que a Bronze não terminou de escrever antes da Silver rodar — reveja o
  passo 7). Depois, `"Linhas antes: X | depois de remover duplicatas: Y"` — Y
  não deve ser drasticamente menor que X (se colapsar quase tudo, é o mesmo
  bug que já apareceu aqui: identificador de linha errado).
- **Gold:** `"Total: 29 tabelas Gold exportadas"` no final.

Se quiser conferir visualmente, dá pra abrir o S3 e ver se as pastas/arquivos
Parquet foram criados nos caminhos esperados (`data/bronze/...`,
`data/silver/...`, `data/gold/...`).

## 9. Catalogando a Gold com Glue Crawler + consultando no Athena

Isso cria as tabelas no **Glue Data Catalog** a partir do Parquet da Gold, pra
poder consultar com SQL no Athena.

### 9.1. Criar o crawler

1. **AWS Glue → Crawlers → Create crawler**.
2. **Set crawler properties:** dê um nome (ex: sufixo da sua conta, tipo
   `<seu-account-id>`).
3. **Choose data sources and classifiers:** adicione uma fonte **S3**, apontando
   pro caminho da Gold **inteiro** (não uma tabela por vez — o crawler descobre
   as 29 sozinho):
   ```
   s3://<seu-bucket>/data/gold/
   ```
   Parâmetro: **Recrawl all**.
4. **Configure security settings:** em IAM role, escolha **Use another role** e
   selecione a `LabRole` da sua conta (a mesma usada nos jobs Glue) — mais
   simples do que criar uma role nova só pro crawler.
5. **Set output and scheduling:**
   - Target database: crie um banco novo (**Add database**), ex:
     `db_gold-<seu-account-id>`.
   - Table name prefix / Maximum table threshold: deixe em branco.
   - **Advanced options → S3 schema grouping:** deixe **"Create a single schema
     for each S3 path" DESMARCADO** — se marcar, o crawler tenta juntar tudo
     numa tabela só, e você quer uma tabela por sub-pergunta (29 no total).
   - **Table level: `5`**. Esse número é importante — é a profundidade de
     pasta (a partir da raiz do bucket) onde o crawler decide "aqui começa uma
     tabela". Contando as pastas do caminho
     `data/gold/<pergunta>/<tabela>/ano_pesquisa=2024/`:
     `data`(1) → `gold`(2) → `<pergunta>`(3) → `<tabela>`(4) →
     `ano_pesquisa=...`(5). Nível 5 é o que faz o crawler parar **antes** da
     pasta de partição e criar uma tabela por `<tabela>` (com `ano_pesquisa`
     virando coluna de partição) — não uma tabela por partição.
   - Schedule: **On demand** (não precisa de agendamento automático).
6. **Review and update / Create crawler**, depois **Run crawler**.

Ao terminar, confira em **Glue → Data Catalog → Databases → Tables**: deve
aparecer o banco (`db_gold-...`) com **29 tabelas**, todas classificação
`Parquet`, apontando pra dentro de `data/gold/`.

### 9.2. Consultando no Athena

1. Abra o **Athena** (mesma conta/região).
2. **Antes da primeira consulta**, configure o local de saída dos resultados
   (obrigatório, senão a query falha com erro de "output location"): aba
   **Configurações de consultas → Criptografia de resultados de consultas →
   Gerenciar**, e aponte pra uma pasta no seu bucket, ex:
   ```
   s3://<seu-bucket>/Athena_consultas/
   ```
3. Na aba **Editor**: Fonte de dados = `AwsDataCatalog`, Banco de dados =
   `db_gold-<seu-account-id>` (o banco criado pelo crawler). As 29 tabelas
   aparecem na lateral, marcadas como **Particionado**.
4. Teste com uma query simples, ex:
   ```sql
   SELECT genero, total, ano_pesquisa
   FROM proporcao_genero
   ORDER BY ano_pesquisa, genero;
   ```
   Se as 3 edições (2023/2024/2025) já rodaram Gold, essa query já deve trazer
   as 3 juntas, uma por `ano_pesquisa` — é o resultado de todo o pipeline
   funcionando ponta a ponta.

> **Nota:** se depois de catalogar você rodar a Gold de uma edição nova depois
> de já ter catalogado (nova partição `ano_pesquisa`), o Glue Catalog pode não
> pegar a partição nova sozinho — rode o crawler de novo (**Run crawler**) pra
> atualizar, ou use `MSCK REPAIR TABLE <nome_tabela>` direto no Athena.

## 10. Conectando o Power BI na Gold (via ODBC)

Com as tabelas já catalogadas (passo 9), dá pra ler a Gold direto no Power BI
usando o driver ODBC oficial do Athena — sem precisar exportar CSV nem passar
por outra ferramenta no meio.

### 10.1. Instalar o driver

1. Baixe o **Athena ODBC 1.x driver** (Simba) na página oficial da AWS:
   [docs.aws.amazon.com — Athena ODBC 1.x driver](https://docs.aws.amazon.com/athena/latest/ug/connect-with-odbc-driver-and-documentation-download-links.html).
   Escolha o instalador Windows 64-bit (ou 32-bit, se seu Power BI for 32-bit).
2. Instale normalmente (`.msi`).

### 10.2. Criar o DSN (ODBC Data Source)

1. Abra **Administrador de Fonte de Dados ODBC (64 bits)** no Windows.
2. Aba **DSN de Sistema → Adicionar → Simba Athena ODBC Driver**.
3. Configure:
   - **Nome do DSN:** um nome que ajude a lembrar, ex: `AthenaGold`.
   - **Região da AWS:** a mesma do seu bucket/Glue (ex: `us-east-1`).
   - **S3 Output Location:** a mesma pasta de resultado que você configurou no
     Athena no passo 9.2, ex: `s3://<seu-bucket>/Athena_consultas/`.
   - **Catalog:** `AwsDataCatalog`.
   - **Schema/Database:** o banco criado pelo crawler, ex:
     `db_gold-<seu-account-id>`.
4. Em **Authentication Options**, escolha **Authentication Type: IAM
   Credentials** e preencha com as credenciais **temporárias** da sua sessão
   ativa do AWS Academy Lab (clique em **AWS Details** no Lab pra ver):
   - **User:** `aws_access_key_id`
   - **Password:** `aws_secret_access_key`
   - **Session Token:** `aws_session_token`

   > ⚠️ **Atenção:** essas 3 credenciais são temporárias e **expiram quando a
   > sessão do Lab encerra** (o cronômetro "Remaining session time" no canto do
   > Lab). Quando expirar, o Power BI para de conectar até você atualizar o DSN
   > com as credenciais novas da sessão seguinte — não tem como deixar
   > permanente num Academy Lab. **Nunca compartilhe/printe essas 3 informações
   > em conversas, tickets ou repositórios** — com elas, qualquer pessoa acessa
   > sua sessão AWS enquanto ela estiver ativa.
5. **OK** pra salvar o DSN.

### 10.3. Conectar no Power BI

1. Power BI Desktop → **Obter dados → Mais → Outros → ODBC** (ou procure
   "ODBC" na busca de conectores).
2. Selecione o DSN criado (`AthenaGold`).
3. No **Navegador**, o Power BI mostra o catálogo `AwsDataCatalog` → banco
   `db_gold-<seu-account-id>` → as **29 tabelas**. Marque as que quiser (dá
   pra marcar todas de uma vez, ou uma a uma) e **Carregar** (ou
   **Transformar Dados**, se quiser tratar algo antes de carregar).
4. Cada tabela já vem com a coluna `ano_pesquisa` — como o crawler catalogou o
   caminho compartilhado das 3 edições, uma tabela como `proporcao_genero`
   traz 2023/2024/2025 juntas na mesma consulta, prontas pra montar um
   gráfico comparativo por ano direto no Power BI.




