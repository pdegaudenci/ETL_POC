import requests
from typing import List, Dict
from datetime import datetime, timezone


class ApiDownloader:
    def __init__(self, base_url: str, http_client=None, source_name: str = "open_meteo"):
        self.base_url = base_url
        self.http_client = http_client or requests
        self.source_name = source_name

    def download(self, limit: int = 100) -> List[Dict]:
        response = self.http_client.get(self.base_url, timeout=30)
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

        max_rows = min(len(times), len(temperatures), limit)

        for i in range(max_rows):
            rows.append({
                "datetime": times[i],
                "temperature_2m": temperatures[i],
                "source": self.source_name,
                "ingestion_date": ingestion_date,
                "ingested_at": ingested_at,
            })

        return rows