from google.api_core.exceptions import NotFound
from google.cloud import bigquery


class BigQueryTransformer:
    """
    Capa TRANSFORM del pipeline.

    Responsabilidad:
    - Crear el dataset INTEGRATION si no existe.
    - Crear la tabla final si no existe.
    - Ejecutar el archivo SQL de transformación.
    - Delegar el procesamiento pesado a BigQuery Query Jobs.

    La tabla final se crea con buenas prácticas:
    - Particionada por execution_date.
    - Clusterizada por symbol.

    La transformación final es idempotente mediante MERGE.
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

    def ensure_integration_table(self, dataset_id: str, table_id: str):
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
            bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("execution_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
        ]

        try:
            self.client.get_table(full_table_id)
            print(f"Tabla INTEGRATION ya existe: {full_table_id}")

        except NotFound:
            table = bigquery.Table(full_table_id, schema=schema)

            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="execution_date",
            )

            table.clustering_fields = ["symbol"]

            self.client.create_table(table)

            print(
                "Tabla INTEGRATION creada con particionado por execution_date "
                f"y clustering por symbol: {full_table_id}"
            )

    def run_sql_file(self, sql_path: str):
        with open(sql_path, "r", encoding="utf-8") as file:
            sql = file.read()

        sql = sql.replace("{project_id}", self.project_id)

        job = self.client.query(sql)
        job.result()

        print("SQL de transformación ejecutado correctamente")