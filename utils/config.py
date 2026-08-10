"""
Configuração central de caminhos do projeto.

A raiz do projeto é calculada de forma dinâmica a partir da localização
deste arquivo (utils/config.py está sempre 1 nível abaixo da raiz).
Isso faz os caminhos funcionarem independente de onde o notebook é
executado (01_bronze/, 02_silver/, 03_gold/, 04_visualizacao.ipynb) e
independente de quem/onde clonou o repositório.
"""

from pathlib import Path

# utils/config.py -> parent = utils/ -> parent.parent = raiz do projeto
RAIZ_PROJETO = Path(__file__).resolve().parent.parent

# --- Raw ---------------------------------------------------------------
CAMINHO_RAW = RAIZ_PROJETO / "data" / "raw"

# --- Bronze --------------------------------------------------------------
CAMINHO_BRONZE = RAIZ_PROJETO / "data" / "bronze" / "state_of_data"
CAMINHO_BRONZE_METADADOS = RAIZ_PROJETO / "data" / "bronze" / "metadados"

# --- Silver --------------------------------------------------------------
CAMINHO_SILVER = RAIZ_PROJETO / "data" / "silver" / "state_of_data_silver"
CAMINHO_SILVER_METADADOS = RAIZ_PROJETO / "data" / "silver" / "metadados"

# --- Gold ------------------------------------------------------------------
CAMINHO_GOLD_BASE = RAIZ_PROJETO / "data" / "gold"

if __name__ == "__main__":
    # Rode `python utils/config.py` pra conferir se os caminhos batem
    print("RAIZ_PROJETO:", RAIZ_PROJETO)
    print("CAMINHO_RAW:", CAMINHO_RAW)
    print("CAMINHO_BRONZE:", CAMINHO_BRONZE)
    print("CAMINHO_BRONZE_METADADOS:", CAMINHO_BRONZE_METADADOS)
    print("CAMINHO_SILVER:", CAMINHO_SILVER)
    print("CAMINHO_SILVER_METADADOS:", CAMINHO_SILVER_METADADOS)
    print("CAMINHO_GOLD_BASE:", CAMINHO_GOLD_BASE)
