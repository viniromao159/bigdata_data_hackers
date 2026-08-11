# State of Data Hackers — Engenharia de Dados e Analytics

**Pós Tech Data Analytics — FIAP**
Tech Challenge Fase 3 — Grupo 6

Desafio da fase: desenvolver uma solução de Engenharia de Dados e Analytics,
aplicando conceitos de Big Data e Analytics ao longo de todo o ciclo de vida
dos dados — da ingestão bruta até a geração de insights de negócio.

Base analisada: pesquisas **State of Data Brazil**, edições **2023, 2024 e
2025**, cada uma tratada de ponta a ponta por um integrante do grupo.

## Equipe

| Nome | GitHub | Edição da pesquisa |
|---|---|---|
| Carlos Henrique Freitas | [@Finnagun](https://github.com/Finnagun) | 2023 |
| Maycon Suel da Silva Nunes | [@MayconNune](https://github.com/MayconNune) | 2024 |
| Vinicius Lopes Romão | [@viniromao159](https://github.com/viniromao159) | 2025 |

## Arquitetura

O pipeline segue o padrão **medallion** (Bronze → Silver → Gold), com cada
edição da pesquisa processada de forma independente nas 3 camadas e depois
unificada na etapa de visualização:

```
raw (CSV bruto por edição)
  → Bronze (schema padronizado, particionado por ano_pesquisa)
    → Silver (regras de negócio aplicadas: escopo de pergunta condicional,
               blocos de múltipla escolha convertidos pra boolean,
               categorias de resposta corrigidas)
      → Gold (tabelas de resposta às 7 perguntas de negócio do desafio,
               uma pasta por pergunta)
        → 04_visualizacao.ipynb (lê a Gold já unificada dos 3 anos,
                                   gera os gráficos finais)
```

Cada camada é escrita no **mesmo caminho**, particionada por `ano_pesquisa`,
com `partitionOverwriteMode = "dynamic"` — assim cada integrante escreve só
a partição do seu próprio ano, sem apagar o trabalho dos outros dois.

Ver `diagrams/arquitetura.drawio` para o diagrama completo.

## Perguntas de negócio (camada Gold)

| Pasta | Pergunta |
|---|---|
| `p1_estrutura_mercado` | Como o mercado de dados está estruturado hoje? |
| `p2_perfis_valorizados` | Quais perfis são mais valorizados (senioridade, experiência, cargo)? |
| `p3_diversidade` | Como estão a diversidade e a equidade salarial? |
| `p4_tecnologias` | Quais tecnologias dominam o mercado? |
| `p5_ia_generativa` | Como a IA generativa está sendo adotada? |
| `p6_diferencas_regionais` | Existem diferenças regionais relevantes? |
| `p7_oportunidades_desafios` | Quais são as oportunidades e desafios reportados? |

Detalhe de cada consulta em `sql/perguntas.sql`.

## Estrutura do repositório

```
data/
├── raw/                          (CSV original de cada edição)
├── bronze/
│   ├── state_of_data/            (particionado por ano_pesquisa)
│   └── metadados/                (de-para código → coluna, por edição)
├── silver/
│   ├── state_of_data_silver/     (particionado por ano_pesquisa)
│   └── metadados/                (dicionário de colunas por edição)
└── gold/
    ├── p1_estrutura_mercado/ ... p7_oportunidades_desafios/
        (cada tabela particionada por ano_pesquisa)

notebooks/
├── 01_bronze/      (bronze_<edição>.ipynb, um por integrante)
├── 02_silver/      (silver_<edição>.ipynb, um por integrante)
├── 03_gold/        (gold_<edição>.ipynb, um por integrante)
└── 04_visualizacao.ipynb   (lê a Gold unificada, gera os gráficos finais)

sql/
└── perguntas.sql   (as 7 perguntas de negócio + consultas de referência)

diagrams/
└── arquitetura.drawio

presentation/
└── TechChallenge.pdf   (relatório executivo — entrega final do desafio)

utils/
├── config.py       (caminhos centrais do projeto: raw/bronze/silver/gold)
├── functions.py    (funções compartilhadas entre as 3 camadas — ver abaixo)
└── constants.py     (constantes de ordenação e conversão — ver abaixo)

requirements.txt
README.md
.gitignore
```

## `utils/` — código compartilhado entre as edições

Pra evitar que cada edição (2023/Carlos, 2024/Maycon, 2025/Vini) reimplemente
a mesma lógica de um jeito ligeiramente diferente, o que é puramente
mecânico (sem regra de negócio específica de uma base) fica centralizado:

- **`config.py`** — caminhos centrais do projeto (`CAMINHO_RAW`,
  `CAMINHO_BRONZE`, `CAMINHO_SILVER`, `CAMINHO_GOLD_BASE`, etc.), resolvidos
  dinamicamente a partir da raiz do repositório.
- **`functions.py`** — tratamento de nome de coluna bruto
  (`extrair_codigo_e_descricao`), referência segura de coluna
  (`col_segura`), normalização de texto (`slug`), e a conversão de blocos
  de múltipla escolha de texto `'1'`/`'0'` pra boolean respeitando o
  escopo da pergunta (`obter_bloco`, `padronizar_bloco`).
- **`constants.py`** — ordem lógica de senioridade (`ordem_senioridade`),
  conversão de faixa salarial pra valor em R$ (`ponto_medio_salarial`,
  `ordem_salarial`) e ordem de tempo de experiência
  (`ordem_tempo_experiencia`).

O que **não** fica em `utils/` é a regra de negócio específica de cada
edição — por exemplo, qual bloco de pergunta usa qual flag de escopo
(`aplica_analise_emprego`, `aplica_analise_tecnica`, `aplica_analise_gestor`)
continua documentado e implementado dentro do notebook Silver de cada
edição, porque isso é fruto da investigação de nulo daquela base
especificamente e pode não valer 1:1 se o formato da pesquisa mudar de ano
pra ano.

## Como rodar

<!-- TODO: ambiente de execução (local vs AWS Academy Lab) ainda não
     definido pelo grupo — atualizar esta seção quando decidido. -->

Por enquanto, os notebooks rodam localmente via Jupyter:

```bash
pip install -r requirements.txt
jupyter lab
```

Rode as camadas na ordem: `01_bronze` → `02_silver` → `03_gold` (dentro de
cada uma, qualquer edição pode rodar independente das outras, já que cada
uma escreve só a sua partição de `ano_pesquisa`) → `04_visualizacao.ipynb`.

## Licença

Projeto acadêmico (Pós Tech Data Analytics — FIAP), utilizando uma base de
dados pública (pesquisa State of Data Brazil). Sem licença de software
aplicada.
