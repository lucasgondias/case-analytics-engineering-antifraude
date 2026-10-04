"""Paridade PySpark x dbt e regras críticas do case.

Rodar a partir de fraud-risk-analytics/ depois de `dbt seed && dbt build`:
    python -m pytest spark/tests -q
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from pyspark.sql import functions as F

from fraud_risk_spark import pipeline as P
from fraud_risk_spark import transformations as T
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
    # Com a sobreposição de 6h (B1), outras transações recentes também são reprocessadas, de forma
    # idempotente; a garantia é que a tx do chargeback novo está entre elas.
    initial_updated_at = attempts.agg({"_updated_at": "max"}).first()[0]
    changed = [r.transaction_id for r in new_attempts.collect() if r._updated_at > initial_updated_at]
    assert "tx_1004" in changed, "a tx que recebeu o chargeback é reprocessada"
    assert new_attempts.count() == attempts.count(), "merge por chave não duplica linhas"

    card = new_cohort.where("payment_method = 'credit_card'").first()
    assert card.chargeback_amount == Decimal("500.00")  # 150 (D+14) + 350 (D+120)
    assert card.approved_amount == Decimal("850.00")

    pix_before = cohort.where("payment_method = 'pix'").first()
    pix_after = new_cohort.where("payment_method = 'pix'").first()
    assert (pix_after.approved_amount, pix_after.chargeback_amount) == (
        pix_before.approved_amount,
        pix_before.chargeback_amount,
    ), "reprocessar a sobreposição não altera valores (idempotência)"


def _tx_rows(spark, rows):
    cols = ["transaction_id", "user_id", "transaction_at", "_ingested_at"]
    return spark.createDataFrame(rows, cols).select(
        "transaction_id",
        "user_id",
        F.to_timestamp("transaction_at").alias("transaction_at"),
        F.to_timestamp("_ingested_at").alias("_ingested_at"),
    )


def test_incremental_picks_out_of_order_events_and_duplicate_neighbors(spark):
    """B1 + B4, mesma fixture do unit test dbt `incremental_picks_out_of_order_...`."""
    tx = _tx_rows(
        spark,
        [
            ("tx_old", "usr_a", "2026-03-01 05:00:00", "2026-03-01 05:00:00"),
            ("tx_late", "usr_b", "2026-03-01 10:59:00", "2026-03-01 11:00:00"),
            ("tx_dup_old", "usr_c", "2026-03-01 12:30:00", "2026-03-01 01:00:00"),
            ("tx_new", "usr_c", "2026-03-01 12:30:00", "2026-03-01 12:30:00"),
        ],
    )
    empty = tx.select("transaction_id", "_ingested_at").limit(0)
    # Watermark obtido pelo próprio Spark (como no pipeline, via collect): um datetime Python
    # ingênuo seria interpretado no fuso da máquina e divergiria do fuso da sessão.
    wm = spark.sql("select timestamp'2026-03-01 12:00:00' as wm").first().wm
    ids = T.affected_transaction_ids(tx, empty, empty, T.Watermarks(wm, wm, wm))
    assert sorted(r.transaction_id for r in ids.collect()) == ["tx_dup_old", "tx_late", "tx_new"]


def test_velocity_features_by_user(spark):
    raw = T.read_raw_csv(spark, str(P.SEEDS_DIR / "raw_transactions.csv"))
    features = T.velocity_features(T.stg_transactions(raw))
    rows = {
        r.transaction_id: (r.user_attempts_10m, r.user_attempts_24h, r.user_amount_24h)
        for r in features.collect()
    }
    assert rows == {
        "tx_1001": (0, 0, Decimal("0.00")),
        "tx_1002": (0, 0, Decimal("0.00")),
        "tx_1003": (0, 1, Decimal("150.00")),  # tx_1001 15 min antes: fora de 10 min, dentro de 24 h
        "tx_1004": (1, 1, Decimal("350.00")),  # tx_1005 no mesmo segundo conta nas duas janelas
        "tx_1005": (1, 1, Decimal("350.00")),
    }
