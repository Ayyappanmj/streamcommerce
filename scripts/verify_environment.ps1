# Checks the tools needed by StreamCommerce. Read-only; changes nothing.
$ErrorActionPreference = "Continue"
function Check($name, $cmd, $args_) {
    $found = Get-Command $cmd -ErrorAction SilentlyContinue
    if ($found) { $v = & $cmd @args_ 2>&1 | Select-Object -First 1; Write-Host "[OK]      $name : $v" }
    else { Write-Host "[MISSING] $name ($cmd not on PATH)" -ForegroundColor Yellow }
}
Check "Python (3.11+ required)" "python" @("--version")
Check "Docker"                  "docker" @("--version")
Check "Docker Compose"          "docker" @("compose", "version")
Check "Java (PySpark engine, 17 recommended)" "java" @("-version")
Check "Git"                     "git"    @("--version")
if (Test-Path ".venv\Scripts\python.exe") {
    & .\.venv\Scripts\python.exe -c "import importlib.util as u; mods=['pandas','pyarrow','duckdb','pyspark','confluent_kafka','psycopg','streamlit','plotly','dbt','pytest']; [print(('[OK]      ' if u.find_spec(m) else '[MISSING] ')+'python module '+m) for m in mods]"
} else { Write-Host "[INFO]    .venv not created yet - run scripts\setup.ps1" }
if (-not (Test-Path ".env")) { Write-Host "[INFO]    .env not found - setup.ps1 creates it from .env.example" }
