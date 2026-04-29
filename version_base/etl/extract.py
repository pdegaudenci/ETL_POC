from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
import json
import requests


class ApiExtractor:
    """
    Capa EXTRACT.

    - Descarga datos desde CoinGecko.
    - Devuelve registros normalizados.
    - Guarda copia local en carpeta output/.
    """

    def __init__(self, api_url: str, source_name: str = "coingecko", http_client=None):
        if not api_url:
            raise ValueError("API_URL no está configurada")

        self.api_url = api_url
        self.source_name = source_name
        self.http_client = http_client or requests

    def extract(self, limit: int = 100) -> List[Dict]:
        response = self.http_client.get(self.api_url, timeout=30)
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list):
            raise ValueError("CoinGecko no devolvió una lista válida")

        now = datetime.now(timezone.utc)
        ingestion_date = now.date().isoformat()
        ingested_at = now.isoformat()

        rows = []

        for item in data[:limit]:
            rows.append(
                {
                    "coin_id": item.get("id"),
                    "symbol": item.get("symbol"),
                    "name": item.get("name"),
                    "current_price": item.get("current_price"),
                    "market_cap": item.get("market_cap"),
                    "market_cap_rank": item.get("market_cap_rank"),
                    "total_volume": item.get("total_volume"),
                    "price_change_percentage_24h": item.get(
                        "price_change_percentage_24h"
                    ),
                    "last_updated": item.get("last_updated"),
                    "source": self.source_name,
                    "ingestion_date": ingestion_date,
                    "ingested_at": ingested_at,
                }
            )

        self.save_to_output(rows)

        return rows

    def save_to_output(self, rows: List[Dict]):
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = output_dir / f"{self.source_name}_markets_{ts}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, ensure_ascii=False)

        print(f"Archivo local generado: {file_path}")