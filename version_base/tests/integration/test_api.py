import os
import json
import pytest
from api.api_downloader import ApiDownloader


REAL_API_URL = os.getenv(
    "API_URL",
    "https://api.open-meteo.com/v1/forecast?latitude=40.4&longitude=-3.7&hourly=temperature_2m&forecast_days=5"
)


@pytest.mark.integration
def test_real_api_download():
    downloader = ApiDownloader(REAL_API_URL)

    result = downloader.download(limit=100)

    assert isinstance(result, list)
    assert len(result) > 0

    os.makedirs("tests/fixture", exist_ok=True)

    file_path = "tests/fixture/api_response.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"Archivo generado: {file_path}")