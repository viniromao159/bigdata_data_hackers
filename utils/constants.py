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
