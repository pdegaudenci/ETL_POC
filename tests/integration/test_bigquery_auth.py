import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery

# Cargar .env desde raíz del proyecto
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")


def main():
    project_id = os.getenv("PROJECT_ID")
    location = os.getenv("BQ_LOCATION", "EU")
    dataset_id = os.getenv("SANDBOX_DATASET")
    table_id = os.getenv("SANDBOX_TABLE")

    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    cred_full = ROOT / cred_path

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(cred_full)

    client = bigquery.Client(project=project_id, location=location)

    sql = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{table_id}` (
      datetime TIMESTAMP,
      temperature_2m FLOAT64,
      execution_date DATE,
      source STRING
    );
    """

    job = client.query(sql)
    job.result()

    print("OK tabla creada.")


if __name__ == "__main__":
    main()