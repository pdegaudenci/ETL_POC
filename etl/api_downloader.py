import requests
from typing import List, Dict


class ApiDownloader:
    def __init__(self, base_url: str, http_client=None):
        self.base_url = base_url
        self.http_client = http_client or requests

    def download(self, limit: int = 100) -> List[Dict]:
        response = self.http_client.get(self.base_url, timeout=30)
        response.raise_for_status()

        data = response.json()

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])

        if not times or not temperatures:
            raise ValueError("La API no devolvió datos horarios válidos")

        rows = []

        for i in range(min(len(times), len(temperatures), limit)):
            rows.append({
                "datetime": times[i],
                "temperature_2m": temperatures[i],
                "source": "open_meteo"
            })

        return rows