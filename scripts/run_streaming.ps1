# Streaming demo. -Transport file replays events without a broker; kafka needs: docker compose --profile streaming up -d
param([ValidateSet("kafka","file")][string]$Transport = "kafka", [int]$Orders = 200, [int]$Duration = 30, [switch]$Faults)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$argsList = @("--transport", $Transport, "--orders", $Orders, "--duration", $Duration)
if ($Faults) { $argsList += "--faults" }
& .\.venv\Scripts\python.exe -m src.ingestion.streaming_demo @argsList
exit $LASTEXITCODE
