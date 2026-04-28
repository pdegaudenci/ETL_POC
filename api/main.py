from api.api_downloader import ApiDownloader
from api.bigquery_loader import BigQueryLoader


PROJECT_ID = "TU_PROJECT_ID"
APP_NAME = "PRUEBA_API"
DATASET_ID = f"SANDBOX_{APP_NAME}"
TABLE_ID = "posts"

API_URL = "https://jsonplaceholder.typicode.com/posts"


def main():
    api = ApiDownloader(API_URL)
    rows = api.download(limit=100)

    bq = BigQueryLoader(project_id=PROJECT_ID)
    bq.ensure_dataset(DATASET_ID)
    bq.load_json_rows(DATASET_ID, TABLE_ID, rows)

    print("Proceso finalizado correctamente")


if __name__ == "__main__":
    main()