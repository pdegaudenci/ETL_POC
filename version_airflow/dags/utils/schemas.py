"""
schemas.py

Definición centralizada de esquemas BigQuery.

Responsabilidad:
- Documentar la estructura esperada de las tablas.
- Reutilizable para validaciones futuras.
- Facilita mantenimiento del modelo de datos.

Nota:
En la versión actual del DAG las tablas se crean mediante SQL
(CREATE TABLE IF NOT EXISTS), por lo que este archivo sirve como
documentación técnica y posible reutilización futura.
"""

# ==========================================================
# SANDBOX TABLE SCHEMA (RAW LAYER)
# ==========================================================
# Tabla de aterrizaje con datos crudos desde CoinGecko
# Incluye campos técnicos de ingesta.
# ==========================================================

SANDBOX_SCHEMA = [
    {"name": "coin_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "current_price", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "market_cap", "type": "INT64", "mode": "NULLABLE"},
    {"name": "market_cap_rank", "type": "INT64", "mode": "NULLABLE"},
    {"name": "total_volume", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "price_change_percentage_24h", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "last_updated", "type": "STRING", "mode": "NULLABLE"},
    {"name": "source", "type": "STRING", "mode": "REQUIRED"},
    {"name": "vs_currency", "type": "STRING", "mode": "REQUIRED"},
    {"name": "ingestion_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]


# ==========================================================
# INTEGRATION TABLE SCHEMA (BUSINESS LAYER)
# ==========================================================
# Tabla final limpia e idempotente lista para reporting,
# analytics o consumo posterior.
# ==========================================================

INTEGRATION_SCHEMA = [
    {"name": "coin_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "current_price", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "market_cap", "type": "INT64", "mode": "NULLABLE"},
    {"name": "market_cap_rank", "type": "INT64", "mode": "NULLABLE"},
    {"name": "total_volume", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "price_change_percentage_24h", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "last_updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "snapshot_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "source", "type": "STRING", "mode": "REQUIRED"},
]