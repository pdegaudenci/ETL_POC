from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from typing import List, Dict, Optional
import traceback

from google.cloud import bigquery
from api.utils.gcp_clients import get_bigquery_client
from api.utils.logger import log_info, log_error
from api.ingestion.ingestion_logger import log_step  # tu logger
from api.ingestion.audit import audit_core_event
from uuid import uuid4


# =====================================================
# CONFIG
# =====================================================
BQ_LOCATION = "EU"
BQ_PROJECT = None   # usa el project del Service Account


# =====================================================
# CLIENT
# =====================================================
def _get_client() -> bigquery.Client:
    """
    Cliente BigQuery reutilizable
    """
    try:
        bq_client = get_bigquery_client()
        """log_step(
            "BIGQUERY",
            "CLIENT",
            f"BigQuery client created (project={bq_client.project}, location={BQ_LOCATION})",
        )"""
        return bq_client
    except Exception as e:
        log_step(
            "BIGQUERY",
            "ERROR",
            f"Failed creating BigQuery client: {e}",
        )
        log_step("BIGQUERY", "ERROR", traceback.format_exc())
        raise


# =====================================================
# DATASET (CORE / MART)
# =====================================================
def ensure_dataset(dataset_id: str):
    """
    Crea dataset CORE o MART si no existe.
    """
    client = _get_client()
    full_dataset_id = f"{client.project}.{dataset_id}"

    #log_step("BIGQUERY", "DATASET", f"Checking dataset {full_dataset_id}")

    try:
        client.get_dataset(full_dataset_id)
        log_step("BIGQUERY", "DATASET", f"Dataset exists: {full_dataset_id}")

    except NotFound:
        try:
            layer = "mart" if dataset_id.endswith("_mart") else "core"

            dataset_ref = bigquery.Dataset(full_dataset_id)
            dataset_ref.location = BQ_LOCATION
            dataset_ref.labels = {
                "layer": layer,
                "managed_by": "datafloud",
            }

            client.create_dataset(dataset_ref)

            log_step(
                "BIGQUERY",
                "DATASET",
                f"Dataset created: {full_dataset_id} (layer={layer})",
            )

        except Exception as e:
            log_step(
                "BIGQUERY",
                "ERROR",
                f"Failed creating dataset {full_dataset_id}: {e}",
            )
            log_step("BIGQUERY", "ERROR", traceback.format_exc())
            raise

    except Exception as e:
        log_step(
            "BIGQUERY",
            "ERROR",
            f"Error checking dataset {full_dataset_id}: {e}",
        )
        log_step("BIGQUERY", "ERROR", traceback.format_exc())
        raise



# =====================================================
# TABLE (CORE / MART)
# =====================================================
def ensure_table(
    table_id: str,
    schema: List[bigquery.SchemaField],
    layer: str = "core",
    partition_field: Optional[str] = None,
    cluster_fields: Optional[List[str]] = None,
):
    client = _get_client()

    # log_step("BIGQUERY", "TABLE", f"Checking table {table_id}")

    try:
        client.get_table(table_id)
        # log_step("BIGQUERY", "TABLE", f"Table exists: {table_id}")
        return

    except NotFound:
        pass
        #log_step("BIGQUERY", "TABLE", f"Table not found, creating: {table_id}")

    table = bigquery.Table(table_id, schema=schema)

    if partition_field: 
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )

    if cluster_fields:
        table.clustering_fields = cluster_fields

    table.labels = {
        "layer": layer,
        "managed_by": "datafloud",
    }

    client.create_table(table)

    log_step(
        "BIGQUERY",
        "TABLE",
        f"Table created: {table_id} (layer={layer})",
    )



# =====================================================
# HISTORICAL LOAD (LEGACY)
# =====================================================
def load_historical_from_gcs(
    gcs_uri: str,
    table_id: str,
    write_disposition: str = "WRITE_TRUNCATE",
):
    """
    ⚠️ LEGACY — mantenido por compatibilidad
    """
    client = _get_client()
    """
    log_step(
        "BIGQUERY",
        "LEGACY_LOAD",
        f"Loading GCS → CORE (legacy)",
    )
    log_step(
        "BIGQUERY",
        "LEGACY_LOAD",
        f"GCS URI: {gcs_uri}",
    )
    log_step(
        "BIGQUERY",
        "LEGACY_LOAD",
        f"Target table: {table_id}",
    )
    """
    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition,
            ignore_unknown_values=True,
        )

        job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config,
        )

        job.result()

        log_step(
            "BIGQUERY",
            "LEGACY_LOAD",
            f"Legacy load completed: {table_id}",
        )

    except Exception as e:
        log_step(
            "BIGQUERY",
            "ERROR",
            f"Legacy load failed for {table_id}: {e}",
        )
        log_step("BIGQUERY", "ERROR", traceback.format_exc())
        raise


# =====================================================
# SQL EXECUTION (STAGE → CORE / MART)
# =====================================================

def run_sql(
    sql: str,
    params: Optional[Dict[str, object]] = None,
    *,
    run_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
    source: str = "unknown",
    entity: str = "unknown",
    phase: str = "CORE",
    env: str = "test",
):
    client = get_bigquery_client()

    # Fallbacks seguros
    run_id = run_id or str(uuid4())
    tenant_id = tenant_id or "unknown"

    # log_step("SQL", "EXECUTE", "Starting BigQuery SQL execution")

    job_config = None

    if params:
        query_params = []

        for k, v in params.items():
            if isinstance(v, str):
                ptype = "STRING"
            elif isinstance(v, bool):
                ptype = "BOOL"
            elif isinstance(v, int):
                ptype = "INT64"
            elif isinstance(v, float):
                ptype = "FLOAT64"
            else:
                raise ValueError(
                    f"Unsupported query param type for {k}: {type(v)}"
                )

            query_params.append(
                bigquery.ScalarQueryParameter(k, ptype, v)
            )

        job_config = bigquery.QueryJobConfig(
            query_parameters=query_params
        )

        #log_step("SQL", "PARAMS", f"Query params: {params}")
    else:
        # log_step("SQL", "PARAMS", "No query params")
        pass

    # ============================
    # AUDIT → START 
    # ============================
    audit_core_event(
        run_id=run_id,
        tenant_id=tenant_id,
        source=source,
        entity=entity,
        phase=phase,
        status="START",
        executed_sql=sql,
        env=env,
    )

    try:
        job = client.query(sql, job_config=job_config)

        #log_step("SQL", "JOB", f"Job started: {job.job_id}")

        result = job.result()

        log_step(
            "SQL",
            "SUCCESS",
            f"Job finished successfully: {job.job_id}",
        )

        # ============================
        # AUDIT → SUCCESS
        # ============================
        audit_core_event(
            run_id=run_id,
            tenant_id=tenant_id,
            source=source,
            entity=entity,
            phase=phase,
            status="SUCCESS",
            job_id=job.job_id,
            rows_affected=getattr(result, "total_rows", None),
            env=env,
        )

        return result

    except Exception as e:
        log_error(
            "SQL execution failed",
            {
                "error": str(e),
                "params": params,
            }
        )

        # ============================
        # AUDIT → ERROR
        # ============================
        audit_core_event(
            run_id=run_id,
            tenant_id=tenant_id,
            source=source,
            entity=entity,
            phase=phase,
            status="ERROR",
            error=str(e),
            env=env,
        )

        raise



# =====================================================
# WEBHOOK INSERT (APPEND)
# =====================================================
def insert_webhook_rows(
    table_id: str,
    rows: List[Dict],
    *,
    run_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
    source: str = "webhook",
    entity: str = "unknown",
    env: str = "test",
):
    if not rows:
        log_step("BIGQUERY", "WEBHOOK", "No rows to insert")
        return

    client = _get_client()

    run_id = run_id or str(uuid4())
    tenant_id = tenant_id or "unknown"

    log_step(
        "BIGQUERY",
        "WEBHOOK",
        f"Inserting {len(rows)} rows into {table_id}",
    )

    audit_core_event(
        run_id=run_id,
        tenant_id=tenant_id,
        source=source,
        entity=entity,
        phase="WEBHOOK_INSERT",
        status="START",
        env=env,
    )

    try:
        errors = client.insert_rows_json(
            table_id,
            rows,
            row_ids=[None] * len(rows),
        )

        if errors:
            raise RuntimeError(errors)

        log_step(
            "BIGQUERY",
            "WEBHOOK",
            f"Inserted {len(rows)} rows into {table_id}",
        )

        audit_core_event(
            run_id=run_id,
            tenant_id=tenant_id,
            source=source,
            entity=entity,
            phase="WEBHOOK_INSERT",
            status="SUCCESS",
            rows_affected=len(rows),
            env=env,
        )

    except Exception as e:
        log_step(
            "BIGQUERY",
            "ERROR",
            f"Webhook insert failed for {table_id}: {e}",
        )
        log_step("BIGQUERY", "ERROR", traceback.format_exc())

        audit_core_event(
            run_id=run_id,
            tenant_id=tenant_id,
            source=source,
            entity=entity,
            phase="WEBHOOK_INSERT",
            status="ERROR",
            error=str(e),
            env=env,
        )

        raise


# =====================================================
# WEBHOOK MERGE
# =====================================================
def merge_webhook_rows(
    dataset_id: str,
    target_table: str,
    staging_table: str,
    merge_sql: str,
    *,
    run_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
    source: str = "webhook",
    entity: str = "unknown",
    env: str = "test",
):
    client = _get_client()

    run_id = run_id or str(uuid4())
    tenant_id = tenant_id or "unknown"

    query = merge_sql.format(
        dataset=dataset_id,
        target=target_table,
        staging=staging_table,
    )

    log_step(
        "BIGQUERY",
        "MERGE",
        f"Running MERGE into {dataset_id}.{target_table}",
    )

    audit_core_event(
        run_id=run_id,
        tenant_id=tenant_id,
        source=source,
        entity=entity,
        phase="WEBHOOK_MERGE",
        status="START",
        executed_sql=query,
        env=env,
    )

    try:
        job = client.query(query)
        job.result()

        log_step(
            "BIGQUERY",
            "MERGE",
            f"MERGE completed for {dataset_id}.{target_table}",
        )

        audit_core_event(
            run_id=run_id,
            tenant_id=tenant_id,
            source=source,
            entity=entity,
            phase="WEBHOOK_MERGE",
            status="SUCCESS",
            job_id=job.job_id,
            env=env,
        )

    except Exception as e:
        log_step(
            "BIGQUERY",
            "ERROR",
            f"MERGE failed for {dataset_id}.{target_table}: {e}",
        )
        log_step("BIGQUERY", "ERROR", traceback.format_exc())

        audit_core_event(
            run_id=run_id,
            tenant_id=tenant_id,
            source=source,
            entity=entity,
            phase="WEBHOOK_MERGE",
            status="ERROR",
            error=str(e),
            env=env,
        )

        raise
