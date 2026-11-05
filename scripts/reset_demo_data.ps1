# Deletes GENERATED demo data only: data\raw, data\processed, data\warehouse, data\quarantine, data\metadata.db.
# Keeps .gitkeep files, data\sample and everything outside .\data. Requires explicit confirmation.
param([switch]$Yes)
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
if (-not $Yes) {
    $answer = Read-Host "This permanently deletes generated demo data under '$root\data'. Type RESET to continue"
    if ($answer -cne "RESET") { Write-Host "Aborted. Nothing was deleted."; exit 1 }
}
foreach ($dir in "raw", "processed", "warehouse", "quarantine") {
    $path = Join-Path $root "data\$dir"
    if (Test-Path $path) {
        Get-ChildItem -LiteralPath $path -Force | Where-Object { $_.Name -ne ".gitkeep" } | Remove-Item -Recurse -Force
    }
}
$meta = Join-Path $root "data\metadata.db"
if (Test-Path $meta) { Remove-Item -LiteralPath $meta -Force }
Write-Host "Demo data reset."
