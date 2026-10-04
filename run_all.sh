#!/usr/bin/env bash
# Roda o case inteiro no Linux/macOS: ambiente, dbt, testes, simulação e PySpark.
# Uso: ./run_all.sh            (SKIP_SPARK=1 ./run_all.sh para pular o PySpark)
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
proj="$root/fraud-risk-analytics"

[ -x "$root/.venv/bin/python" ] || python3 -m venv "$root/.venv"
py="$root/.venv/bin/python"
"$py" -m pip install -q --upgrade pip
"$py" -m pip install -q -r "$proj/requirements.txt"

export DBT_PROFILES_DIR="$proj"
cd "$proj"
rm -f fraud_risk.duckdb
"$root/.venv/bin/dbt" seed --quiet
"$root/.venv/bin/dbt" build --exclude resource_type:seed
"$py" scripts/simulate_late_chargeback.py
"$root/.venv/bin/sqlfluff" lint models tests/singular
if [ "${SKIP_SPARK:-0}" != "1" ] && command -v java >/dev/null; then
  "$py" -m pytest spark/tests -q -p no:cacheprovider
fi
echo "Tudo certo. Abra case-analytics-engineering-fraude.html e guia-conceitos-fraude-e-risco.html."
