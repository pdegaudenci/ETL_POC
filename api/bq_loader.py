from typing import List, Dict
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


class BigQueryLoader:
    def __init__(self, project_id: str, location: str = "EU"):
        self.project_id = project_id
        self.location = location
        self.client = bigquery.Client(project=project_id)

    def ensure_dataset(self, dataset_id: str):
        full_dataset_id = f"{self.project_id}.{dataset_id}"

        try:
            self.client.get_dataset(full_dataset_id)
            print(f"Dataset ya existe: {full_dataset_id}")
        except NotFound:
            dataset = bigquery.Dataset(full_dataset_id)
            dataset.location = self.location
            self.client.create_dataset(dataset)
            print(f"Dataset creado: {full_dataset_id}")

    def load_json_rows(
        self,
        dataset_id: str,
        table_id: str,
        rows: List[Dict],
        write_disposition: str = "WRITE_TRUNCATE"
    ):
        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition,
            ignore_unknown_values=True,
        )

        job = self.client.load_table_from_json(
            rows,
            full_table_id,
            job_config=job_config,
        )

        job.result()
        print(f"Datos cargados en BigQuery: {full_table_id}")

    def run_sql_file(self, sql_path: str):
        with open(sql_path, "r", encoding="utf-8") as file:
            sql = file.read()

        job = self.client.query(sql)
        job.result()

        print("SQL ejecutado correctamente")