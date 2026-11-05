# Unit + data quality + idempotency + integration tests that need no external service; then Ruff.
param([switch]$External)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
& .\.venv\Scripts\python.exe -m ruff check .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if ($External) { & .\.venv\Scripts\python.exe -m pytest } else { & .\.venv\Scripts\python.exe -m pytest -m "not external" }
exit $LASTEXITCODE
