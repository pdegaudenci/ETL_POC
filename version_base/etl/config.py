import os
from pathlib import Path
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]

load_dotenv(ROOT_DIR / ".env")


class Settings:
    """
    Clase de configuración centralizada.

    Lee las variables de entorno desde el archivo .env y expone
    los valores necesarios para ejecutar el pipeline ETL.

    Permite parametrizar:
    - Proyecto GCP
    - Ubicación BigQuery
    - Dataset y tabla SANDBOX
    - Dataset y tabla INTEGRATION
    - URL de la API
    - Límite de registros
    - Credenciales de Google Cloud
    - Ruta del SQL de transformación
    """

    PROJECT_ID = os.getenv("PROJECT_ID")
    BQ_LOCATION = os.getenv("BQ_LOCATION", "EU")

    APP_NAME = os.getenv("APP_NAME", "CRYPTO")

    SANDBOX_DATASET = os.getenv("SANDBOX_DATASET", f"SANDBOX_{APP_NAME}")
    SANDBOX_TABLE = os.getenv("SANDBOX_TABLE", "coingecko_markets")

    INTEGRATION_DATASET = os.getenv("INTEGRATION_DATASET", "INTEGRATION")
    INTEGRATION_TABLE = os.getenv(
        "INTEGRATION_TABLE",
        "integration_prueba_tecnica"
    )

    API_URL = os.getenv("API_URL")
    API_LIMIT = int(os.getenv("API_LIMIT", "100"))
    SOURCE_NAME = os.getenv("SOURCE_NAME", "coingecko")

    WRITE_DISPOSITION = os.getenv("WRITE_DISPOSITION", "WRITE_TRUNCATE")

    GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "credentials/service_account.json"
    )

    GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH = str(
        ROOT_DIR / GOOGLE_APPLICATION_CREDENTIALS
    )

    SQL_TRANSFORM_PATH = str(ROOT_DIR / "sql" / "transform.sql")


settings = Settings()