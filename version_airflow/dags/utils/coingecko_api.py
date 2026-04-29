from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from google.cloud import bigquery

from utils.config import settings


def _safe_float(value: Any):
    """
    Convierte valores numéricos a FLOAT de forma segura.

    Si el valor viene como None, string inválido o tipo no convertible,
    devuelve None para evitar errores durante la carga en BigQuery.
    """
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any):
    """
    Convierte valores numéricos a INT de forma segura.

    Si el valor viene como None, string inválido o tipo no convertible,
    devuelve None para evitar errores durante la carga en BigQuery.
    """
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_market_row(item: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    """
    Normaliza un registro devuelto por CoinGecko.

    Esta función convierte el JSON original de CoinGecko en una estructura
    plana compatible con la tabla SANDBOX de BigQuery.

    También añade campos técnicos:
    - source
    - vs_currency
    - ingestion_date
    - ingested_at
    """

    return {
        "coin_id": item.get("id"),
        "symbol": item.get("symbol"),
        "name": item.get("name"),
        "current_price": _safe_float(item.get("current_price")),
        "market_cap": _safe_int(item.get("market_cap")),
        "market_cap_rank": _safe_int(item.get("market_cap_rank")),
        "total_volume": _safe_float(item.get("total_volume")),
        "price_change_percentage_24h": _safe_float(
            item.get("price_change_percentage_24h")
        ),
        "last_updated": item.get("last_updated"),
        "source": settings.SOURCE_NAME,
        "vs_currency": settings.API_VS_CURRENCY,
        "ingestion_date": ingested_at.date().isoformat(),
        "ingested_at": ingested_at.isoformat(),
    }


def fetch_coingecko_market_data(**context) -> int:
    """
    Task de extracción para Airflow.

    Responsabilidad:
    - Llamar a la API pública de CoinGecko.
    - Descargar los datos de mercado.
    - Normalizar la respuesta.
    - Validar registros mínimos obligatorios.
    - Guardar los registros normalizados en XCom.

    Devuelve:
    - Número de registros válidos descargados.
    """

    params = {
        "vs_currency": settings.API_VS_CURRENCY,
        "order": settings.API_ORDER,
        "per_page": settings.API_LIMIT,
        "page": settings.API_PAGE,
        "sparkline": settings.API_SPARKLINE,
    }

    response = requests.get(settings.API_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise ValueError("CoinGecko API response must be a list")

    if not data:
        raise ValueError("CoinGecko API returned no rows")

    ingested_at = datetime.now(timezone.utc)

    rows = [
        normalize_market_row(item, ingested_at)
        for item in data[: int(settings.API_LIMIT)]
    ]

    valid_rows = [
        row
        for row in rows
        if row.get("coin_id")
        and row.get("symbol")
        and row.get("current_price") is not None
    ]

    if not valid_rows:
        raise ValueError("No valid rows after CoinGecko normalization")

    context["ti"].xcom_push(
        key="coingecko_rows",
        value=valid_rows,
    )

    return len(valid_rows)


def load_rows_to_bigquery(**context) -> int:
    """
    Task de carga para Airflow.

    Responsabilidad:
    - Recuperar los registros normalizados desde XCom.
    - Cargar los datos en la tabla SANDBOX de BigQuery.
    - Delegar la escritura a un BigQuery Load Job.

    Devuelve:
    - Número de registros cargados.
    """

    rows: List[Dict[str, Any]] = context["ti"].xcom_pull(
        task_ids="extract_coingecko_data",
        key="coingecko_rows",
    )

    if not rows:
        raise ValueError("No rows found in XCom from extract_coingecko_data")

    client = bigquery.Client(
        project=settings.PROJECT_ID1,
        location=settings.BQ_LOCATION,
    )

    table_id = (
        f"{settings.PROJECT_ID1}."
        f"{settings.SANDBOX_DATASET}."
        f"{settings.SANDBOX_TABLE}"
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=settings.WRITE_DISPOSITION,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=True,
    )

    job = client.load_table_from_json(
        rows,
        table_id,
        job_config=job_config,
    )

    job.result()

    return len(rows)