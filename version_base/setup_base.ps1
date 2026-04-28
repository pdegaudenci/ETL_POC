$ErrorActionPreference = "Stop"

Write-Host "Creating virtual environment for version_base..."
python -m venv venv

Write-Host "Activating virtual environment..."
.\venv\Scripts\Activate.ps1

Write-Host "Upgrading pip..."
python -m pip install --upgrade pip setuptools wheel

Write-Host "Installing project requirements..."
pip install -r requirements.txt

Write-Host "Setup completed."
Write-Host "Now edit your .env file with PROJECT_ID and credentials path."
Write-Host "Then run: .\run_base.ps1"