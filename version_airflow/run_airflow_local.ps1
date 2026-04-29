$ErrorActionPreference = "Stop"

.\venv\Scripts\Activate.ps1

$env:AIRFLOW_HOME = (Get-Location).Path

Write-Host "Starting Airflow standalone..."
airflow standalone