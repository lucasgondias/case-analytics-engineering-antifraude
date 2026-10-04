# Roda o case inteiro no Windows: ambiente, dbt, testes, simulação e PySpark.
# Uso (PowerShell, na raiz do repositório):  .\run_all.ps1
# Opção: -SkipSpark (pula PySpark, que exige Java 8+)
param([switch]$SkipSpark)
$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$proj = Join-Path $root "fraud-risk-analytics"
$venv = Join-Path $root ".venv"
$py = Join-Path $venv "Scripts\python.exe"
$dbt = Join-Path $venv "Scripts\dbt.exe"

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Check($what) { if ($LASTEXITCODE -ne 0) { throw "Falhou: $what (exit $LASTEXITCODE)" } }

if (-not (Test-Path $py)) {
    Step "Criando ambiente virtual em .venv"
    python -m venv $venv; Check "python -m venv"
}
Step "Instalando dependências"
& $py -m pip install -q --upgrade pip; Check "pip upgrade"
& $py -m pip install -q -r (Join-Path $proj "requirements.txt"); Check "pip install"

$env:DBT_PROFILES_DIR = $proj
$env:PYTHONIOENCODING = "utf-8"
Push-Location $proj
try {
    Step "dbt seed (Bronze com os dados do case)"
    Remove-Item -Force -ErrorAction SilentlyContinue "fraud_risk.duckdb"
    & $dbt seed --quiet; Check "dbt seed"

    Step "dbt build (modelos + testes de dado + unit tests). Esperado: PASS=109 WARN=2"
    & $dbt build --exclude resource_type:seed; Check "dbt build"

    Step "Simulação: chargeback D+120 reabre só a safra afetada"
    & $py scripts\simulate_late_chargeback.py; Check "simulação"

    Step "Lint SQL"
    & (Join-Path $venv "Scripts\sqlfluff.exe") lint models tests\singular; Check "sqlfluff"

    if (-not $SkipSpark) {
        if (Get-Command java -ErrorAction SilentlyContinue) {
            Step "PySpark: paridade com o dbt (cerca de 30s)"
            & $py -m pytest spark\tests -q -p no:cacheprovider; Check "pytest spark"
        } else {
            Write-Host "Java não encontrado no PATH: pulando PySpark (instale JDK 8+ ou use -SkipSpark)" -ForegroundColor Yellow
        }
    }
} finally {
    Pop-Location
}

Step "Tudo certo"
