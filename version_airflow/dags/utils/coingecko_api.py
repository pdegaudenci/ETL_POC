from datetime import datetime, timezone
from typing import Any, Dict, List
import json

import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from utils.config import settings


# ==========================================================
# HELPERS
# ==========================================================

def _safe_float(value: Any):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ==========================================================
# NORMALIZATION
# ==========================================================

def normalize_market_row(item: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    """
    Convierte respuesta CoinGecko en fila compatible con BigQuery.
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


# ==========================================================
# TASK 1 - EXTRACT
# ==========================================================

def fetch_coingecko_market_data(**context) -> int:
    """
    Descarga datos desde CoinGecko y los deja en XCom.
    """

    params = {
        "vs_currency": settings.API_VS_CURRENCY,
        "order": settings.API_ORDER,
        "per_page": int(settings.API_LIMIT),
        "page": int(settings.API_PAGE),
        "sparkline": settings.API_SPARKLINE,
    }

    response = requests.get(
        settings.API_URL,
        params=params,
        timeout=30,
    )
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
        raise ValueError("No valid rows after normalization")

    context["ti"].xcom_push(
        key="coingecko_rows",
        value=valid_rows,
    )

    return len(valid_rows)


# ==========================================================
# TASK 2 - WRITE RAW FILE TO GCS
# ==========================================================

def write_raw_json_to_gcs(**context) -> str:
    """
    Guarda archivo NDJSON en Cloud Storage.

    Ruta:
    gs://bucket/data/coingecko/markets/YYYY-MM-DD/file.ndjson
    """

    rows: List[Dict[str, Any]] = context["ti"].xcom_pull(
        task_ids="extract_coingecko_data",
        key="coingecko_rows",
    )

    if not rows:
        raise ValueError("No rows found in XCom from extract_coingecko_data")

    now = datetime.now(timezone.utc)

    date_folder = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%d_%H%M%S")

    object_name = (
        f"{settings.RAW_PREFIX}/"
        f"{date_folder}/"
        f"coingecko_markets_{ts}.ndjson"
    )

    content = "\n".join(
        json.dumps(row, ensure_ascii=False, default=str)
        for row in rows
    )

    hook = GCSHook()

    hook.upload(
        bucket_name=settings.RAW_BUCKET,
        object_name=object_name,
        data=content,
        mime_type="application/x-ndjson",
    )

    context["ti"].xcom_push(
        key="gcs_object_name",
        value=object_name,
    )

    return object_name

