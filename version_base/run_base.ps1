$ErrorActionPreference = "Stop"

.\venv\Scripts\Activate.ps1

$env:PYTHONPATH = (Get-Location).Path

Write-Host "Running base ETL pipeline..."
python -m etl.main