import os
from pathlib import Path

from etl.config import settings
from etl.extract import ApiExtractor
from etl.load import BigQueryLoader
from etl.transform import BigQueryTransformer


def validate_settings():
    if not settings.PROJECT_ID:
        raise ValueError("Falta PROJECT_ID en .env")

    if not settings.API_URL:
        raise ValueError("Falta API_URL en .env")

    if not settings.GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH:
        raise ValueError("Falta GOOGLE_APPLICATION_CREDENTIALS en .env")

    if not Path(settings.GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH).exists():
        raise FileNotFoundError(
            "No existe el archivo de credenciales: "
            f"{settings.GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH}"
        )


def main():
    validate_settings()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        settings.GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH
    )

    print("EXTRACT - Descargando datos desde API...")
    extractor = ApiExtractor(
        api_url=settings.API_URL,
        source_name=settings.SOURCE_NAME,
    )
    rows = extractor.extract(limit=settings.API_LIMIT)
    print(f"EXTRACT - Registros descargados: {len(rows)}")

    print("LOAD - Preparando BigQuery SANDBOX...")
    loader = BigQueryLoader(
        project_id=settings.PROJECT_ID,
        location=settings.BQ_LOCATION,
    )

    loader.ensure_dataset(settings.SANDBOX_DATASET)

    loader.ensure_sandbox_table(
        dataset_id=settings.SANDBOX_DATASET,
        table_id=settings.SANDBOX_TABLE,
    )

    loader.load_json_rows(
        dataset_id=settings.SANDBOX_DATASET,
        table_id=settings.SANDBOX_TABLE,
        rows=rows,
        write_disposition=settings.WRITE_DISPOSITION,
    )

    print("TRANSFORM - Preparando BigQuery INTEGRATION...")
    transformer = BigQueryTransformer(
        project_id=settings.PROJECT_ID,
        location=settings.BQ_LOCATION,
    )

    transformer.ensure_dataset(settings.INTEGRATION_DATASET)

    transformer.ensure_integration_table(
        dataset_id=settings.INTEGRATION_DATASET,
        table_id=settings.INTEGRATION_TABLE,
    )

    print("TRANSFORM - Ejecutando SQL idempotente...")
    transformer.run_sql_file(settings.SQL_TRANSFORM_PATH)

    print("Proceso ETL finalizado correctamente.")


if __name__ == "__main__":
    main()