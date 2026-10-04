"""Paridade PySpark x dbt e regras críticas do case.

Rodar a partir de fraud-risk-analytics/ depois de `dbt seed && dbt build`:
    python -m pytest spark/tests -q
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from fraud_risk_spark import pipeline as P
from fraud_risk_spark.session import local_session

DUCKDB_PATH = Path(__file__).resolve().parents[2] / "fraud_risk.duckdb"

ATTEMPT_COLUMNS = [
    "transaction_id",
    "cohort_date",
    "status",
    "payment_method",
    "is_chargeback_eligible",
    "decline_source",
    "risk_score",
    "decision_source",
    "has_chargeback",
    "chargeback_amount",
    "days_to_first_chargeback",
    "is_approved_without_risk_evaluation",
    "is_duplicate_candidate",
    "duplicate_of_transaction_id",
]

COHORT_COLUMNS = [
    "cohort_date",
    "payment_method",
    "attempt_count",
    "approved_count",
    "approved_amount",
    "chargeback_transaction_count",
    "chargeback_amount",
    "fraud_chargeback_amount",
    "cohort_age_days",
    "is_mature",
]


@pytest.fixture(scope="session")
def spark():
    session = local_session("fraud-risk-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def initial(spark):
    return P.initial_load(spark)


def _spark_rows(df, columns, order):
    return [tuple(r) for r in df.select(*columns).orderBy(*order).collect()]


def _dbt_rows(table, columns, order):
    if not DUCKDB_PATH.exists():
        pytest.skip("rode `dbt seed && dbt build` antes para gerar fraud_risk.duckdb")
    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as con:
        sql = f"select {', '.join(columns)} from {table} order by {', '.join(order)}"
        return [tuple(r) for r in con.execute(sql).fetchall()]


def test_payment_attempts_match_dbt(initial):
    attempts, _, _ = initial
    order = ["transaction_id"]
    assert _spark_rows(attempts, ATTEMPT_COLUMNS, order) == _dbt_rows(
        "marts.fct_payment_attempts", ATTEMPT_COLUMNS, order
    )


def test_cohort_aggregate_matches_dbt(initial):
    _, cohort, _ = initial
    order = ["cohort_date", "payment_method"]
    assert _spark_rows(cohort, COHORT_COLUMNS, order) == _dbt_rows(
        "marts.agg_chargeback_cohort_daily", COHORT_COLUMNS, order
    )


def test_silent_failures_are_flagged_not_dropped(initial):
    attempts, _, _ = initial
    rows = {r.transaction_id: r for r in attempts.collect()}
    assert len(rows) == 5, "nenhuma transação pode ser descartada"
    assert rows["tx_1005"].is_approved_without_risk_evaluation
    assert rows["tx_1005"].is_duplicate_candidate
    assert rows["tx_1005"].duplicate_of_transaction_id == "tx_1004"
    assert rows["tx_1002"].decline_source == "risk_engine"
    assert rows["tx_1004"].decision_source == "manual_review"
    assert rows["tx_1003"].risk_score == 5  # '05' tipado


def test_late_chargeback_reopens_only_affected_cohort(spark, initial):
    attempts, cohort, watermarks = initial
    new_attempts, new_cohort, cohorts = P.incremental_run(
        spark, attempts, cohort, watermarks, [P.LATE_CHARGEBACK]
    )
    assert [c.isoformat() for c in cohorts] == ["2026-03-01"]

    # Comparação em Python (não literal SQL): independente do fuso da máquina e da sessão Spark.
    initial_updated_at = attempts.agg({"_updated_at": "max"}).first()[0]
    changed = [r.transaction_id for r in new_attempts.collect() if r._updated_at > initial_updated_at]
    assert changed == ["tx_1004"], "só a tx afetada é reprocessada"

    card = new_cohort.where("payment_method = 'credit_card'").first()
    assert card.chargeback_amount == Decimal("500.00")  # 150 (D+14) + 350 (D+120)
    assert card.approved_amount == Decimal("850.00")

    pix_before = cohort.where("payment_method = 'pix'").first()
    pix_after = new_cohort.where("payment_method = 'pix'").first()
    assert pix_after._updated_at == pix_before._updated_at, "partição não afetada não é reescrita"
