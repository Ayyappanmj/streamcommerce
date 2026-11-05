# Run the batch pipeline. Default: lightweight (pandas engine). Use -Engine spark for PySpark, -WithDbt to include dbt.
param([int]$Orders = 10000, [int]$Seed = 42, [ValidateSet("pandas","spark")][string]$Engine = "pandas",
      [switch]$WithDbt, [switch]$Faults, [switch]$NoGenerate, [switch]$Force)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$argsList = @("--engine", $Engine, "--orders", $Orders, "--seed", $Seed)
if (-not $NoGenerate) { $argsList += "--generate" }
if ($WithDbt) { $argsList += "--with-dbt" }
if ($Faults)  { $argsList += "--faults" }
if ($Force)   { $argsList += "--force" }
& .\.venv\Scripts\python.exe -m src.batch_pipeline @argsList
exit $LASTEXITCODE
