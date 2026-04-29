from typing import Dict, List

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


class BigQueryLoader:
    """
    Capa LOAD del pipeline.

    Responsabilidad:
    - Crear el dataset SANDBOX si no existe.
    - Crear la tabla SANDBOX si no existe.
    - Cargar los datos crudos descargados desde CoinGecko.

    La tabla SANDBOX se crea con buenas prácticas de BigQuery:
    - Particionada por ingestion_date.
    - Clusterizada por symbol.

    Esta clase no transforma datos finales. Solo carga datos raw/sandbox.
    """

    def __init__(self, project_id: str, location: str = "EU"):
        if not project_id:
            raise ValueError("PROJECT_ID no está configurado")

        self.project_id = project_id
        self.location = location
        self.client = bigquery.Client(
            project=project_id,
            location=location,
        )

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

    def ensure_sandbox_table(self, dataset_id: str, table_id: str):
        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"

        schema = [
            bigquery.SchemaField("coin_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("symbol", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("current_price", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("market_cap", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("market_cap_rank", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("total_volume", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField(
                "price_change_percentage_24h",
                "FLOAT64",
                mode="NULLABLE",
            ),
            bigquery.SchemaField("last_updated", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ingestion_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(full_table_id)
            print(f"Tabla SANDBOX ya existe: {full_table_id}")

        except NotFound:
            table = bigquery.Table(full_table_id, schema=schema)

            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ingestion_date",
            )

            table.clustering_fields = ["symbol"]

            self.client.create_table(table)

            print(
                "Tabla SANDBOX creada con particionado por ingestion_date "
                f"y clustering por symbol: {full_table_id}"
            )

    def load_json_rows(
        self,
        dataset_id: str,
        table_id: str,
        rows: List[Dict],
        write_disposition: str = "WRITE_TRUNCATE",
    ):
        if not rows:
            raise ValueError("No hay registros para cargar en BigQuery")

        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ignore_unknown_values=True,
        )

        job = self.client.load_table_from_json(
            rows,
            full_table_id,
            job_config=job_config,
        )

        job.result()

        print(f"{len(rows)} registros cargados en {full_table_id}")