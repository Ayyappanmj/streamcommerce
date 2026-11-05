# Generate synthetic CSV (and an events JSONL file) into data\raw\incoming.
param([int]$Customers = 1000, [int]$Products = 200, [int]$Orders = 10000, [int]$Seed = 42, [switch]$Faults)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$argsList = @("--customers", $Customers, "--products", $Products, "--orders", $Orders, "--seed", $Seed,
              "--events-file", "data\raw\events.jsonl")
if ($Faults) { $argsList += "--faults" }
& .\.venv\Scripts\python.exe -m src.generators.cli @argsList
