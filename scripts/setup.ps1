# Creates the virtual environment, installs dependencies and creates .env (never overwrites an existing .env).
param([switch]$SkipInstall)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
if (-not (Test-Path ".venv")) { python -m venv .venv }
if (-not $SkipInstall) {
    & .\.venv\Scripts\python.exe -m pip install --upgrade pip
    & .\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
}
if (-not (Test-Path ".env")) {
    Copy-Item .env.example .env
    Write-Host "Created .env from .env.example - edit POSTGRES_PASSWORD if you use PostgreSQL."
}
if (-not (Test-Path "dbt_project\profiles.yml")) { Copy-Item dbt_project\profiles.yml.example dbt_project\profiles.yml }
Write-Host "Setup complete. Activate with: .\.venv\Scripts\Activate.ps1"
