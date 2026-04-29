import os
from pathlib import Path


class Settings:
    """
    Configuración centralizada del DAG de Airflow / Cloud Composer.

    Responsabilidad:
    - Leer variables de entorno o Airflow Environment Variables.
    - Definir nombres de datasets y tablas de BigQuery.
    - Definir parámetros de la API CoinGecko.
    - Resolver la ruta de los archivos SQL dentro del bucket DAGs.
    - Validar configuración mínima antes de cargar el DAG.

    Nota:
    En Cloud Composer es recomendable configurar estos valores como
    Environment Variables del entorno o Airflow Variables, según la estrategia
    del proyecto. Esta clase usa variables de entorno mediante os.getenv().
    """

    PROJECT_ID = os.getenv("PROJECT_ID1")
    BQ_LOCATION = os.getenv("BQ_LOCATION", "EU")

    APP_NAME = os.getenv("APP_NAME", "PRUEBA_CRYPTO")

    SANDBOX_DATASET = os.getenv("SANDBOX_DATASET", f"SANDBOX_{APP_NAME}")
    SANDBOX_TABLE = os.getenv("SANDBOX_TABLE", "coingecko_markets_raw")

    INTEGRATION_DATASET = os.getenv("INTEGRATION_DATASET", "INTEGRATION")
    INTEGRATION_TABLE = os.getenv(
        "INTEGRATION_TABLE",
        "integration_coingecko_markets",
    )

    API_URL = os.getenv(
        "API_URL",
        "https://api.coingecko.com/api/v3/coins/markets",
    )

    API_VS_CURRENCY = os.getenv("API_VS_CURRENCY", "usd")
    API_ORDER = os.getenv("API_ORDER", "market_cap_desc")
    API_LIMIT = int(os.getenv("API_LIMIT", "100"))
    API_PAGE = int(os.getenv("API_PAGE", "1"))
    API_SPARKLINE = os.getenv("API_SPARKLINE", "false")

    SOURCE_NAME = os.getenv("SOURCE_NAME", "coingecko")
    WRITE_DISPOSITION = os.getenv("WRITE_DISPOSITION", "WRITE_TRUNCATE")

    DAG_DIR = Path(__file__).resolve().parents[1]
    SQL_DIR = DAG_DIR / "sql"

    @classmethod
    def validate(cls):
        """
        Valida que la configuración mínima exista.

        Esta validación se ejecuta al importar el DAG.
        Si falta una variable crítica, Airflow marcará el DAG como broken,
        lo cual facilita detectar errores de configuración rápidamente.
        """

        if not cls.PROJECT_ID:
            raise ValueError("PROJECT_ID is required")

        if not cls.BQ_LOCATION:
            raise ValueError("BQ_LOCATION is required")

        if not cls.SANDBOX_DATASET:
            raise ValueError("SANDBOX_DATASET is required")

        if not cls.SANDBOX_TABLE:
            raise ValueError("SANDBOX_TABLE is required")

        if not cls.INTEGRATION_DATASET:
            raise ValueError("INTEGRATION_DATASET is required")

        if not cls.INTEGRATION_TABLE:
            raise ValueError("INTEGRATION_TABLE is required")

        if not cls.API_URL:
            raise ValueError("API_URL is required")

        if cls.API_LIMIT <= 0:
            raise ValueError("API_LIMIT must be greater than zero")

    @classmethod
    def read_sql(cls, filename: str) -> str:
        """
        Lee un archivo SQL desde la carpeta dags/sql.

        Reemplaza placeholders para evitar hardcodear proyecto, datasets
        o tablas dentro de los scripts SQL.

        Placeholders soportados:
        - {project_id}
        - {sandbox_dataset}
        - {sandbox_table}
        - {integration_dataset}
        - {integration_table}
        """

        sql_path = cls.SQL_DIR / filename

        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")

        sql = sql_path.read_text(encoding="utf-8")

        return (
            sql.replace("{project_id}", cls.PROJECT_ID)
            .replace("{sandbox_dataset}", cls.SANDBOX_DATASET)
            .replace("{sandbox_table}", cls.SANDBOX_TABLE)
            .replace("{integration_dataset}", cls.INTEGRATION_DATASET)
            .replace("{integration_table}", cls.INTEGRATION_TABLE)
        )


settings = Settings()
settings.validate()