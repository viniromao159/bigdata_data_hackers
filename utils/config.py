"""Config Carlos"""

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



"""Config Maycon"""

"""
Configuração da SparkSession.

Local: roda com master local[*], sem nada de AWS.
Glue: quando subir pro Glue Notebook, a SparkSession já vem pronta
(a variável `spark` já existe no ambiente do Glue) -- get_spark_session()
detecta isso e reaproveita, em vez de criar uma nova.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "tech-challenge-state-of-data") -> SparkSession:
    """Retorna uma SparkSession. Se já existir uma ativa (ex: dentro do
    Glue Notebook), reaproveita; senão cria uma local para testes."""
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")  # base pequena, não precisa de 200 partições padrão
        .getOrCreate()
    )