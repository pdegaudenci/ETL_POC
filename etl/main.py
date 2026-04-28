import os
from pathlib import Path

from config import settings
from etl.api_downloader import ApiDownloader
from etl.bq_loader import BigQueryLoader


def validate_settings():
    if not settings.PROJECT_ID:
        raise ValueError("Falta PROJECT_ID en .env")

    if not settings.API_URL:
        raise ValueError("Falta API_URL en .env")

    if not settings.GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH:
        raise ValueError("Falta GOOGLE_APPLICATION_CREDENTIALS en .env")

    if not Path(settings.GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH).exists():
        raise FileNotFoundError(
            f"No existe el archivo de credenciales: "
            f"{settings.GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH}"
        )


def main():
    validate_settings()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        settings.GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH
    )

    print("Descargando datos desde API...")
    downloader = ApiDownloader(settings.API_URL)
    rows = downloader.download(limit=settings.API_LIMIT)
    print(f"Registros descargados: {len(rows)}")

    bq = BigQueryLoader(
        project_id=settings.PROJECT_ID,
        location=settings.BQ_LOCATION,
    )

    print("Creando/verificando dataset SANDBOX...")
    bq.ensure_dataset(settings.SANDBOX_DATASET)

    print("Creando/verificando tabla SANDBOX...")
    bq.ensure_sandbox_table(
        dataset_id=settings.SANDBOX_DATASET,
        table_id=settings.SANDBOX_TABLE,
    )

    print("Cargando datos en SANDBOX...")
    bq.load_json_rows(
        dataset_id=settings.SANDBOX_DATASET,
        table_id=settings.SANDBOX_TABLE,
        rows=rows,
        write_disposition=settings.WRITE_DISPOSITION,
    )

    print("Creando/verificando dataset INTEGRATION...")
    bq.ensure_dataset(settings.INTEGRATION_DATASET)

    print("Creando/verificando tabla INTEGRATION...")
    bq.ensure_integration_table(
        dataset_id=settings.INTEGRATION_DATASET,
        table_id=settings.INTEGRATION_TABLE,
    )

    print("Ejecutando transformación SQL idempotente...")
    sql_path = Path(__file__).resolve().parents[1] / "sql" / "transform.sql"
    bq.run_sql_file(str(sql_path))

    print("Proceso completo finalizado correctamente.")


if __name__ == "__main__":
    main()