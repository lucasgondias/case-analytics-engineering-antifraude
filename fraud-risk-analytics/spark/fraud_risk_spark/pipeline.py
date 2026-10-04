"""Demonstração ponta a ponta em PySpark: carga inicial + chargeback tardio incremental.

Uso (a partir de fraud-risk-analytics/):
    python spark/run_pipeline.py
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fraud_risk_spark import transformations as T

SEEDS_DIR = Path(__file__).resolve().parents[2] / "seeds"

LATE_CHARGEBACK = {
    "chargeback_id": "cb_902",
    "transaction_id": "tx_1004",
    "chargeback_at": "2026-06-29 09:00:00",
    "reason_code": "10.4_fraud",
    "cb_amount": "350.00",
    "_ingested_at": "2026-06-29 09:05:00",
}


def load_staging(spark: SparkSession, extra_chargebacks: list[dict] | None = None):
    raw_tx = T.read_raw_csv(spark, str(SEEDS_DIR / "raw_transactions.csv"))
    raw_ev = T.read_raw_csv(spark, str(SEEDS_DIR / "raw_risk_evaluations.csv"))
    raw_cb = T.read_raw_csv(spark, str(SEEDS_DIR / "raw_chargebacks.csv"))
    codes = T.read_raw_csv(spark, str(SEEDS_DIR / "ref_chargeback_reason_codes.csv"))
    if extra_chargebacks:
        raw_cb = raw_cb.unionByName(spark.createDataFrame(extra_chargebacks).select(*raw_cb.columns))
    return (
        T.stg_transactions(raw_tx),
        T.stg_risk_evaluations(raw_ev),
        T.stg_chargebacks(raw_cb, codes),
    )


def initial_load(spark: SparkSession) -> tuple[DataFrame, DataFrame, T.Watermarks]:
    tx, ev, cb = load_staging(spark)
    attempts = T.build_payment_attempts(tx, ev, cb, updated_at=datetime(2026, 10, 3, 6, 0))
    cohort = T.build_cohort_aggregate(attempts)
    return attempts.cache(), cohort.cache(), T.current_watermarks(tx, ev, cb)


def incremental_run(
    spark: SparkSession,
    attempts: DataFrame,
    cohort: DataFrame,
    since: T.Watermarks,
    extra_chargebacks: list[dict],
) -> tuple[DataFrame, DataFrame, list[date]]:
    tx, ev, cb = load_staging(spark, extra_chargebacks)
    ids = T.affected_transaction_ids(tx, ev, cb, since)
    changed = T.build_payment_attempts(
        tx, ev, cb, only_transaction_ids=ids, updated_at=datetime(2026, 10, 3, 7, 0)
    ).cache()
    new_attempts = T.merge_by_key(attempts, changed, ["transaction_id"]).cache()

    cohorts = T.affected_cohorts(changed)
    new_cohort_rows = T.build_cohort_aggregate(new_attempts, cohorts)
    new_cohort = T.merge_by_key(cohort, new_cohort_rows, ["cohort_date", "payment_method"])
    return new_attempts, new_cohort.cache(), cohorts


def show_cohort(df: DataFrame, title: str) -> None:
    print(f"\n== {title} ==")
    (
        df.select(
            "cohort_date",
            "payment_method",
            "approved_count",
            "approved_amount",
            "chargeback_amount",
            F.round(F.col("chargeback_amount") * 100 / F.col("approved_amount"), 2).alias(
                "cb_rate_amount_pct"
            ),
            "_updated_at",
        )
        .orderBy("cohort_date", "payment_method")
        .show(truncate=False)
    )


def main() -> None:
    from fraud_risk_spark.session import local_session

    spark = local_session()
    attempts, cohort, watermarks = initial_load(spark)
    show_cohort(cohort, "Carga inicial (dados do case)")

    _, new_cohort, cohorts = incremental_run(
        spark, attempts, cohort, watermarks, [LATE_CHARGEBACK]
    )
    print(f"Safras reprocessadas: {[c.isoformat() for c in cohorts]}")
    show_cohort(new_cohort, "Após chargeback D+120 de tx_1004 (incremental por safra afetada)")
    spark.stop()


if __name__ == "__main__":
    main()
