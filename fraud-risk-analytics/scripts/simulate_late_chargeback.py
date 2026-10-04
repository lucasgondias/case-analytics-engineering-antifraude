"""Demonstra o incremental por safra afetada.

1. Mostra o estado atual da safra 2026-03-01.
2. Injeta no Bronze um chargeback de D+120 (fora da janela de 90 dias) para tx_1004.
3. Roda o dbt incremental e mostra que só a safra afetada mudou e que nada se perdeu.
4. Remove o evento simulado (deixa o ambiente como estava, salvo com --keep).

Uso (a partir da raiz do projeto dbt):
    python scripts/simulate_late_chargeback.py [--keep]
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb

PROJECT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_DIR / "fraud_risk.duckdb"
DBT = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")

COHORT_SQL = """
    select cohort_date, payment_method, approved_count, approved_amount,
           chargeback_transaction_count, chargeback_amount,
           round(100.0 * chargeback_amount / nullif(approved_amount, 0), 2) as cb_rate_amount_pct,
           _updated_at
    from marts.agg_chargeback_cohort_daily
    order by 1, 2
"""

LATE_CB = (
    "cb_902",
    "tx_1004",
    "2026-06-29 09:00:00",
    "10.4_fraud",
    "350.00",
    "2026-06-29 09:05:00",
)


def show(title: str) -> None:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        print(f"\n== {title} ==")
        print(con.sql(COHORT_SQL))


def run_dbt(*args: str) -> None:
    env = {**os.environ, "DBT_PROFILES_DIR": str(PROJECT_DIR)}
    cmd = [str(DBT), *args, "--quiet"]
    result = subprocess.run(cmd, cwd=PROJECT_DIR, env=env, check=False)
    if result.returncode != 0:
        raise SystemExit(f"dbt falhou: {' '.join(cmd)}")


def main() -> None:
    keep = "--keep" in sys.argv
    show("ANTES: safra com 1 chargeback (tx_1001)")

    with duckdb.connect(str(DB_PATH)) as con:
        con.execute("delete from raw.raw_chargebacks where chargeback_id = ?", [LATE_CB[0]])
        con.execute("insert into raw.raw_chargebacks values (?, ?, ?, ?, ?, ?)", list(LATE_CB))
    print(f"\nChegou no Bronze: {LATE_CB[0]} para {LATE_CB[1]} em {LATE_CB[2]} (D+120)")

    run_dbt("build", "--exclude", "resource_type:seed")
    show("DEPOIS: incremental reabriu só a safra 2026-03-01 (afetada pelo evento)")

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        print("\n== Chargeback fora da janela de 90 dias continua contabilizado ==")
        print(con.sql(
            "select chargeback_id, transaction_id, cohort_date, days_to_chargeback, "
            "is_beyond_maturity_window from marts.fct_chargebacks order by 1"
        ))

    if not keep:
        with duckdb.connect(str(DB_PATH)) as con:
            con.execute("delete from raw.raw_chargebacks where chargeback_id = ?", [LATE_CB[0]])
        run_dbt("build", "--exclude", "resource_type:seed", "--full-refresh")
        print("\nEvento simulado removido e marts reconstruídos (use --keep para manter).")


if __name__ == "__main__":
    main()
