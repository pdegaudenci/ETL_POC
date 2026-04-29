from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)

from utils.config import settings
from utils.coingecko_api import (
    fetch_coingecko_market_data,
    load_rows_to_bigquery,
)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def table_ref(dataset: str, table: str) -> str:
    return f"`{settings.PROJECT_ID}.{dataset}.{table}`"


def dataset_ref(dataset: str) -> str:
    return f"`{settings.PROJECT_ID}.{dataset}`"


with DAG(
    dag_id="coingecko_market_pipeline",
    description="Professional ETL pipeline using CoinGecko API, Cloud Composer and BigQuery",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["gcp", "bigquery", "composer", "coingecko", "data-engineering"],
) as dag:

    create_sandbox_dataset = BigQueryInsertJobOperator(
        task_id="create_sandbox_dataset",
        location=settings.BQ_LOCATION,
        configuration={
            "query": {
                "query": """
                CREATE SCHEMA IF NOT EXISTS {dataset}
                OPTIONS(location = '{location}');
                """.format(
                    dataset=dataset_ref(settings.SANDBOX_DATASET),
                    location=settings.BQ_LOCATION,
                ),
                "useLegacySql": False,
            }
        },
    )

    create_integration_dataset = BigQueryInsertJobOperator(
        task_id="create_integration_dataset",
        location=settings.BQ_LOCATION,
        configuration={
            "query": {
                "query": """
                CREATE SCHEMA IF NOT EXISTS {dataset}
                OPTIONS(location = '{location}');
                """.format(
                    dataset=dataset_ref(settings.INTEGRATION_DATASET),
                    location=settings.BQ_LOCATION,
                ),
                "useLegacySql": False,
            }
        },
    )

    create_sandbox_table = BigQueryInsertJobOperator(
        task_id="create_sandbox_table",
        location=settings.BQ_LOCATION,
        configuration={
            "query": {
                "query": """
                CREATE TABLE IF NOT EXISTS {table}
                (
                    coin_id STRING,
                    symbol STRING,
                    name STRING,
                    current_price FLOAT64,
                    market_cap INT64,
                    market_cap_rank INT64,
                    total_volume FLOAT64,
                    price_change_percentage_24h FLOAT64,
                    last_updated STRING,
                    source STRING,
                    vs_currency STRING,
                    ingestion_date DATE,
                    ingested_at TIMESTAMP
                )
                PARTITION BY ingestion_date
                CLUSTER BY source, symbol;
                """.format(
                    table=table_ref(
                        settings.SANDBOX_DATASET,
                        settings.SANDBOX_TABLE,
                    )
                ),
                "useLegacySql": False,
            }
        },
    )

    create_integration_table = BigQueryInsertJobOperator(
        task_id="create_integration_table",
        location=settings.BQ_LOCATION,
        configuration={
            "query": {
                "query": """
                CREATE TABLE IF NOT EXISTS {table}
                (
                    coin_id STRING,
                    symbol STRING,
                    name STRING,
                    current_price FLOAT64,
                    market_cap INT64,
                    market_cap_rank INT64,
                    total_volume FLOAT64,
                    price_change_percentage_24h FLOAT64,
                    last_updated TIMESTAMP,
                    snapshot_date DATE,
                    source STRING,
                    vs_currency STRING,
                    execution_date DATE,
                    ingested_at TIMESTAMP
                )
                PARTITION BY snapshot_date
                CLUSTER BY source, symbol;
                """.format(
                    table=table_ref(
                        settings.INTEGRATION_DATASET,
                        settings.INTEGRATION_TABLE,
                    )
                ),
                "useLegacySql": False,
            }
        },
    )

    extract_coingecko_data = PythonOperator(
        task_id="extract_coingecko_data",
        python_callable=fetch_coingecko_market_data,
    )

    load_to_sandbox = PythonOperator(
        task_id="load_to_sandbox",
        python_callable=load_rows_to_bigquery,
    )

    check_sandbox_has_rows = BigQueryCheckOperator(
        task_id="check_sandbox_has_rows",
        location=settings.BQ_LOCATION,
        use_legacy_sql=False,
        sql="""
        SELECT COUNT(*) > 0
        FROM {table}
        WHERE ingestion_date = CURRENT_DATE()
        """.format(
            table=table_ref(
                settings.SANDBOX_DATASET,
                settings.SANDBOX_TABLE,
            )
        ),
    )

    transform_to_integration = BigQueryInsertJobOperator(
        task_id="transform_to_integration",
        location=settings.BQ_LOCATION,
        configuration={
            "query": {
                "query": settings.read_sql("transform_coingecko.sql"),
                "useLegacySql": False,
            }
        },
    )

    check_integration_has_rows = BigQueryCheckOperator(
        task_id="check_integration_has_rows",
        location=settings.BQ_LOCATION,
        use_legacy_sql=False,
        sql="""
        SELECT COUNT(*) > 0
        FROM {table}
        WHERE snapshot_date = CURRENT_DATE()
        """.format(
            table=table_ref(
                settings.INTEGRATION_DATASET,
                settings.INTEGRATION_TABLE,
            )
        ),
    )

    check_no_duplicate_business_key = BigQueryCheckOperator(
        task_id="check_no_duplicate_business_key",
        location=settings.BQ_LOCATION,
        use_legacy_sql=False,
        sql="""
        SELECT COUNT(*) = 0
        FROM (
            SELECT
                coin_id,
                symbol,
                snapshot_date,
                source,
                COUNT(*) AS total
            FROM {table}
            WHERE snapshot_date = CURRENT_DATE()
            GROUP BY coin_id, symbol, snapshot_date, source
            HAVING COUNT(*) > 1
        )
        """.format(
            table=table_ref(
                settings.INTEGRATION_DATASET,
                settings.INTEGRATION_TABLE,
            )
        ),
    )

    run_data_quality_summary = BigQueryInsertJobOperator(
        task_id="run_data_quality_summary",
        location=settings.BQ_LOCATION,
        configuration={
            "query": {
                "query": settings.read_sql("data_quality_checks_coingecko.sql"),
                "useLegacySql": False,
            }
        },
    )

    [create_sandbox_dataset, create_integration_dataset] >> create_sandbox_table
    create_integration_dataset >> create_integration_table

    (
        [create_sandbox_table, create_integration_table]
        >> extract_coingecko_data
        >> load_to_sandbox
        >> check_sandbox_has_rows
        >> transform_to_integration
        >> check_integration_has_rows
        >> check_no_duplicate_business_key
        >> run_data_quality_summary
    )