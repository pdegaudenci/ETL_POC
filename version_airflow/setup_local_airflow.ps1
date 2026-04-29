$ErrorActionPreference = "Stop"

Write-Host "Creating virtual environment..."
python -m venv venv

Write-Host "Activating virtual environment..."
.\venv\Scripts\Activate.ps1

Write-Host "Upgrading pip..."
python -m pip install --upgrade pip setuptools wheel

Write-Host "Installing Apache Airflow..."
pip install "apache-airflow==2.10.5" `
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

Write-Host "Installing Google provider and project requirements..."
pip install apache-airflow-providers-google
pip install -r requirements.txt

Write-Host "Setting AIRFLOW_HOME..."
$env:AIRFLOW_HOME = (Get-Location).Path

Write-Host "Migrating Airflow DB..."
airflow db migrate

Write-Host "Creating local Airflow user..."
airflow users create `
  --username admin `
  --firstname Admin `
  --lastname test `
  --role Admin `
  --email example@hotmail.com `
  --password admin

Write-Host "Setup completed."
Write-Host "Run: .\run_airflow_local.ps1"