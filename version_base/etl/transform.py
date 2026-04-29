from google.api_core.exceptions import NotFound
from google.cloud import bigquery


class BigQueryTransformer:
    """
    Transform layer.

    Responsabilidad:
    - Crear tabla INTEGRATION si no existe.
    - Ejecutar SQL de transformación.
    - Delegar la transformación pesada a BigQuery Query Jobs.
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
            bigquery.SchemaField("datetime", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("temperature_2m", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("execution_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
        ]

        try:
            self.client.get_table(full_table_id)
            print(f"Tabla INTEGRATION ya existe: {full_table_id}")

        except NotFound:
            table = bigquery.Table(full_table_id, schema=schema)

            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="datetime",
            )

            table.clustering_fields = ["source"]

            self.client.create_table(table)

            print(
                "Tabla INTEGRATION creada con particionado por datetime "
                f"y clustering por source: {full_table_id}"
            )

    def run_sql_file(self, sql_path: str):
        with open(sql_path, "r", encoding="utf-8") as file:
            sql = file.read()

        sql = sql.replace("{project_id}", self.project_id)

        job = self.client.query(sql)
        job.result()

        print("SQL de transformación ejecutado correctamente")