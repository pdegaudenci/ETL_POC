import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    PROJECT_ID = os.getenv("PROJECT_ID")
    BQ_LOCATION = os.getenv("BQ_LOCATION", "EU")

    APP_NAME = os.getenv("APP_NAME", "PRUEBA_API")

    SANDBOX_DATASET = os.getenv(
        "SANDBOX_DATASET",
        f"SANDBOX_{APP_NAME}"
    )

    SANDBOX_TABLE = os.getenv("SANDBOX_TABLE", "posts")

    INTEGRATION_DATASET = os.getenv("INTEGRATION_DATASET", "INTEGRATION")
    INTEGRATION_TABLE = os.getenv(
        "INTEGRATION_TABLE",
        "integration_prueba_tecnica"
    )

    API_URL = os.getenv("API_URL")
    API_LIMIT = int(os.getenv("API_LIMIT", "100"))

    WRITE_DISPOSITION = os.getenv("WRITE_DISPOSITION", "WRITE_TRUNCATE")


settings = Settings()