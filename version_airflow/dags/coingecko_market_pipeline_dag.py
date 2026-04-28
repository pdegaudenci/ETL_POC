from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)

from version_airflow.dags.utils.config import settings
from version_airflow.dags.utils.coingecko_api import fetch_coingecko_market_data, load_rows_to_bigquery
from version_airflow.dags.utils.schemas import SANDBOX_SCHEMA, INTEGRATION_SCHEMA


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    dag_id="coingecko_market_pipeline",
    description="Professional ETL pipeline using CoinGecko API, Cloud Composer and BigQuery",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["gcp", "bigquery", "composer", "coingecko", "data-engineering"],
) as dag:

    create_sandbox_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_sandbox_dataset",
        project_id=settings.PROJECT_ID,
        dataset_id=settings.SANDBOX_DATASET,
        location=settings.BQ_LOCATION,
        exists_ok=True,
    )

    create_integration_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_integration_dataset",
        project_id=settings.PROJECT_ID,
        dataset_id=settings.INTEGRATION_DATASET,
        location=settings.BQ_LOCATION,
        exists_ok=True,
    )

    create_sandbox_table = BigQueryCreateEmptyTableOperator(
        task_id="create_sandbox_table",
        project_id=settings.PROJECT_ID,
        dataset_id=settings.SANDBOX_DATASET,
        table_id=settings.SANDBOX_TABLE,
        schema_fields=SANDBOX_SCHEMA,
        time_partitioning={
            "type": "DAY",
            "field": "ingestion_date",
        },
        cluster_fields=["source", "symbol"],
        exists_ok=True,
    )

    create_integration_table = BigQueryCreateEmptyTableOperator(
        task_id="create_integration_table",
        project_id=settings.PROJECT_ID,
        dataset_id=settings.INTEGRATION_DATASET,
        table_id=settings.INTEGRATION_TABLE,
        schema_fields=INTEGRATION_SCHEMA,
        time_partitioning={
            "type": "DAY",
            "field": "snapshot_date",
        },
        cluster_fields=["source", "symbol"],
        exists_ok=True,
    )

    extract_coingecko_data = PythonOperator(
        task_id="extract_coingecko_data",
        python_callable=fetch_coingecko_market_data,
    )

    load_to_sandbox = PythonOperator(
        task_id="load_to_sandbox",
        python_callable=load_rows_to_bigquery,
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
        params={
            "project_id": settings.PROJECT_ID,
            "sandbox_dataset": settings.SANDBOX_DATASET,
            "sandbox_table": settings.SANDBOX_TABLE,
            "integration_dataset": settings.INTEGRATION_DATASET,
            "integration_table": settings.INTEGRATION_TABLE,
        },
    )

    check_sandbox_has_rows = BigQueryCheckOperator(
        task_id="check_sandbox_has_rows",
        location=settings.BQ_LOCATION,
        use_legacy_sql=False,
        sql=f"""
        SELECT COUNT(*) > 0
        FROM `{settings.PROJECT_ID}.{settings.SANDBOX_DATASET}.{settings.SANDBOX_TABLE}`
        WHERE ingestion_date = CURRENT_DATE()
        """,
    )

    check_integration_has_rows = BigQueryCheckOperator(
        task_id="check_integration_has_rows",
        location=settings.BQ_LOCATION,
        use_legacy_sql=False,
        sql=f"""
        SELECT COUNT(*) > 0
        FROM `{settings.PROJECT_ID}.{settings.INTEGRATION_DATASET}.{settings.INTEGRATION_TABLE}`
        WHERE snapshot_date = CURRENT_DATE()
        """,
    )

    check_no_duplicate_business_key = BigQueryCheckOperator(
        task_id="check_no_duplicate_business_key",
        location=settings.BQ_LOCATION,
        use_legacy_sql=False,
        sql=f"""
        SELECT COUNT(*) = 0
        FROM (
            SELECT
                coin_id,
                symbol,
                snapshot_date,
                source,
                COUNT(*) AS total
            FROM `{settings.PROJECT_ID}.{settings.INTEGRATION_DATASET}.{settings.INTEGRATION_TABLE}`
            WHERE snapshot_date = CURRENT_DATE()
            GROUP BY coin_id, symbol, snapshot_date, source
            HAVING COUNT(*) > 1
        )
        """,
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
        params={
            "project_id": settings.PROJECT_ID,
            "sandbox_dataset": settings.SANDBOX_DATASET,
            "sandbox_table": settings.SANDBOX_TABLE,
            "integration_dataset": settings.INTEGRATION_DATASET,
            "integration_table": settings.INTEGRATION_TABLE,
        },
    )

    (
        [create_sandbox_dataset, create_integration_dataset]
        >> create_sandbox_table
        >> create_integration_table
        >> extract_coingecko_data
        >> load_to_sandbox
        >> check_sandbox_has_rows
        >> transform_to_integration
        >> check_integration_has_rows
        >> check_no_duplicate_business_key
        >> run_data_quality_summary
    )