from datetime import datetime, timezone
from typing import Dict, List

import requests


class ApiExtractor:
    """
    Extract layer.

    Responsabilidad:
    - Conectarse a la API pública.
    - Descargar datos.
    - Convertir la respuesta de Open-Meteo en registros planos.
    """

    def __init__(self, api_url: str, source_name: str = "open_meteo", http_client=None):
        if not api_url:
            raise ValueError("API_URL no está configurada")

        self.api_url = api_url
        self.source_name = source_name
        self.http_client = http_client or requests

    def extract(self, limit: int = 100) -> List[Dict]:
        response = self.http_client.get(self.api_url, timeout=30)
        response.raise_for_status()

        data = response.json()

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])

        if not times or not temperatures:
            raise ValueError("La API no devolvió datos horarios válidos")

        now = datetime.now(timezone.utc)
        ingestion_date = now.date().isoformat()
        ingested_at = now.isoformat()

        rows = []

        for i in range(min(len(times), len(temperatures), limit)):
            rows.append(
                {
                    "datetime": times[i],
                    "temperature_2m": temperatures[i],
                    "source": self.source_name,
                    "ingestion_date": ingestion_date,
                    "ingested_at": ingested_at,
                }
            )

        return rows