"""Transformações do mart de Anti-Fraud & Risk em PySpark.

Mesma lógica dos modelos dbt (models/), escrita como funções puras de DataFrame -> DataFrame.
Isso separa REGRA (testável em memória, sem cluster) de I/O (Delta MERGE / replaceWhere, em io_delta.py).

Paridade com o dbt é verificada em tests/test_parity.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
CHARGEBACK_ELIGIBLE_METHODS = ("credit_card", "debit_card")
CLOCK_SKEW_TOLERANCE_SECONDS = 5
DUPLICATE_WINDOW_SECONDS = 60
MATURITY_WINDOW_DAYS = 90


@dataclass(frozen=True)
class Watermarks:
    """Maior _ingested_at já processado por fonte (gravado no próprio fato)."""

    transactions: datetime
    evaluations: datetime
    chargebacks: datetime


# --------------------------------------------------------------------------------------
# Bronze
# --------------------------------------------------------------------------------------
def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    """Lê o Bronze como string (cópia fiel). Tipagem acontece só no staging."""
    return spark.read.option("header", True).option("inferSchema", False).csv(path)


# --------------------------------------------------------------------------------------
# Staging
# --------------------------------------------------------------------------------------
def _latest_by(df: DataFrame, key: str, order_col: str = "_ingested_at") -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def stg_transactions(raw: DataFrame) -> DataFrame:
    typed = raw.select(
        F.trim("transaction_id").alias("transaction_id"),
        F.trim("user_id").alias("user_id"),
        F.to_timestamp("transaction_at", TS_FORMAT).alias("transaction_at"),
        F.to_date(F.to_timestamp("transaction_at", TS_FORMAT)).alias("transaction_date"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.lower(F.trim("status")).alias("status"),
        F.lower(F.trim("payment_method")).alias("payment_method"),
        F.to_timestamp("_ingested_at", TS_FORMAT).alias("_ingested_at"),
    )
    return _latest_by(typed, "transaction_id").withColumns(
        {
            "is_approved": F.col("status") == "approved",
            "is_chargeback_eligible": F.col("payment_method").isin(*CHARGEBACK_ELIGIBLE_METHODS),
        }
    )


def stg_risk_evaluations(raw: DataFrame) -> DataFrame:
    typed = raw.select(
        F.trim("evaluation_id").alias("evaluation_id"),
        F.trim("transaction_id").alias("transaction_id"),
        F.to_timestamp("evaluated_at", TS_FORMAT).alias("evaluated_at"),
        F.col("risk_score").cast("int").alias("risk_score"),  # '05' -> 5
        F.lower(F.trim("risk_action")).alias("risk_action"),
        F.nullif(F.lower(F.trim("rule_triggered")), F.lit("")).alias("rule_triggered"),
        F.to_timestamp("_ingested_at", TS_FORMAT).alias("_ingested_at"),
    )
    return _latest_by(typed, "evaluation_id").withColumn(
        "decision_source",
        F.when(F.col("rule_triggered").startswith("rule_manual_review"), "manual_review").otherwise(
            "automatic"
        ),
    )


def stg_chargebacks(raw: DataFrame, reason_codes: DataFrame) -> DataFrame:
    typed = raw.select(
        F.trim("chargeback_id").alias("chargeback_id"),
        F.trim("transaction_id").alias("transaction_id"),
        F.to_timestamp("chargeback_at", TS_FORMAT).alias("chargeback_at"),
        F.trim("reason_code").alias("reason_code_raw"),
        F.regexp_extract(F.trim("reason_code"), r"^([0-9]+(\.[0-9]+)?)", 1).alias(
            "network_reason_code"
        ),
        F.col("cb_amount").cast("decimal(18,2)").alias("cb_amount"),
        F.to_timestamp("_ingested_at", TS_FORMAT).alias("_ingested_at"),
    )
    codes = reason_codes.select(
        F.col("reason_code").alias("network_reason_code"), "reason_category"
    )
    joined = typed.join(codes, "network_reason_code", "left").withColumn(
        "reason_category", F.coalesce("reason_category", F.lit("unmapped"))
    )
    return _latest_by(joined, "chargeback_id").withColumn(
        "is_fraud_reason", F.col("reason_category") == "fraud"
    )


# --------------------------------------------------------------------------------------
# Intermediate
# --------------------------------------------------------------------------------------
def risk_decision_per_transaction(evaluations: DataFrame, transactions: DataFrame) -> DataFrame:
    """Última avaliação até a autorização (+ tolerância). Re-score posterior não decide."""
    joined = evaluations.join(
        transactions.select("transaction_id", "transaction_at"), "transaction_id"
    ).withColumn(
        "is_pre_authorization",
        F.col("evaluated_at")
        <= F.col("transaction_at") + F.expr(f"INTERVAL {CLOCK_SKEW_TOLERANCE_SECONDS} SECONDS"),
    )
    by_tx = Window.partitionBy("transaction_id")
    ranked = Window.partitionBy("transaction_id").orderBy(
        F.col("is_pre_authorization").desc(),
        F.col("evaluated_at").desc(),
        F.col("evaluation_id").desc(),
    )
    return (
        joined.withColumns(
            {
                "evaluation_count": F.count("*").over(by_tx),
                "post_authorization_evaluation_count": F.sum(
                    F.when(~F.col("is_pre_authorization"), 1).otherwise(0)
                ).over(by_tx),
                "_rn": F.row_number().over(ranked),
            }
        )
        .where("_rn = 1")
        .select(
            "transaction_id",
            "evaluation_id",
            "evaluated_at",
            "risk_score",
            "risk_action",
            "rule_triggered",
            "decision_source",
            "evaluation_count",
            "post_authorization_evaluation_count",
        )
    )


def chargebacks_per_transaction(chargebacks: DataFrame) -> DataFrame:
    return chargebacks.groupBy("transaction_id").agg(
        F.count("*").alias("chargeback_count"),
        F.sum("cb_amount").alias("chargeback_amount"),
        F.sum(F.when(F.col("is_fraud_reason"), F.col("cb_amount"))).alias(
            "fraud_chargeback_amount"
        ),
        F.max(F.col("is_fraud_reason").cast("int")).cast("boolean").alias("has_fraud_chargeback"),
        F.min("chargeback_at").alias("first_chargeback_at"),
    )


def duplicate_suspects(transactions: DataFrame) -> DataFrame:
    """Mesmo usuário, valor e método em <= 60s. Sinaliza; nunca remove."""
    a = transactions.alias("a")
    b = transactions.alias("b")
    pairs = a.join(
        b,
        (F.col("a.user_id") == F.col("b.user_id"))
        & (F.col("a.amount") == F.col("b.amount"))
        & (F.col("a.payment_method") == F.col("b.payment_method"))
        & (
            F.abs(F.unix_timestamp("a.transaction_at") - F.unix_timestamp("b.transaction_at"))
            <= DUPLICATE_WINDOW_SECONDS
        ),
    )
    return (
        pairs.groupBy(F.col("a.transaction_id").alias("transaction_id"))
        .agg(
            F.min("b.transaction_id").alias("first_transaction_id_in_group"),
            F.count("*").alias("group_size"),
        )
        .where("group_size > 1")
        .withColumn(
            "is_duplicate_candidate",
            F.col("transaction_id") != F.col("first_transaction_id_in_group"),
        )
    )


# --------------------------------------------------------------------------------------
# Marts
# --------------------------------------------------------------------------------------
def build_payment_attempts(
    transactions: DataFrame,
    evaluations: DataFrame,
    chargebacks: DataFrame,
    only_transaction_ids: DataFrame | None = None,
    updated_at: datetime | None = None,
) -> DataFrame:
    """fct_payment_attempts. Com only_transaction_ids, constrói só as chaves afetadas."""
    risk = risk_decision_per_transaction(evaluations, transactions)
    cbs = chargebacks_per_transaction(chargebacks)
    dups = duplicate_suspects(transactions)

    base = transactions
    if only_transaction_ids is not None:
        base = base.join(only_transaction_ids.distinct(), "transaction_id", "left_semi")

    has_risk = F.col("risk_action").isNotNull() | F.col("evaluation_id").isNotNull()
    return (
        base.join(risk, "transaction_id", "left")
        .join(cbs, "transaction_id", "left")
        .join(dups.select("transaction_id", "is_duplicate_candidate",
                          "first_transaction_id_in_group"), "transaction_id", "left")
        .select(
            "transaction_id",
            "user_id",
            "transaction_at",
            F.col("transaction_date").alias("cohort_date"),
            "amount",
            "status",
            "payment_method",
            "is_chargeback_eligible",
            "is_approved",
            F.when(F.col("status") == "approved", F.lit(None))
            .when(F.col("status") == "error", "technical_error")
            .when(F.col("risk_action") == "reject", "risk_engine")
            .when(~has_risk, "unknown_no_risk_evaluation")
            .otherwise("issuer_or_acquirer")
            .alias("decline_source"),
            "evaluation_id",
            "risk_score",
            "risk_action",
            "rule_triggered",
            "decision_source",
            F.coalesce("evaluation_count", F.lit(0)).alias("evaluation_count"),
            F.col("chargeback_count").isNotNull().alias("has_chargeback"),
            F.coalesce("has_fraud_chargeback", F.lit(False)).alias("has_fraud_chargeback"),
            F.coalesce("chargeback_count", F.lit(0)).alias("chargeback_count"),
            F.coalesce("chargeback_amount", F.lit(0).cast("decimal(38,2)")).alias(
                "chargeback_amount"
            ),
            F.coalesce("fraud_chargeback_amount", F.lit(0).cast("decimal(38,2)")).alias(
                "fraud_chargeback_amount"
            ),
            "first_chargeback_at",
            F.datediff(F.to_date("first_chargeback_at"), F.to_date("transaction_at")).alias(
                "days_to_first_chargeback"
            ),
            (F.col("is_approved") & ~has_risk).alias("is_approved_without_risk_evaluation"),
            F.coalesce(F.col("is_approved") & (F.col("risk_action") == "reject"), F.lit(False))
            .alias("is_approved_despite_risk_reject"),
            F.coalesce("is_duplicate_candidate", F.lit(False)).alias("is_duplicate_candidate"),
            F.col("first_transaction_id_in_group").alias("duplicate_of_transaction_id"),
            F.lit(updated_at or datetime.now()).cast("timestamp").alias("_updated_at"),
        )
    )


def build_cohort_aggregate(
    attempts: DataFrame,
    as_of_date: date,
    cohorts: Iterable[date] | None = None,
) -> DataFrame:
    """agg_chargeback_cohort_daily. Só numeradores/denominadores aditivos.

    Com `cohorts`, calcula apenas essas safras (usado no reprocessamento incremental).
    """
    if cohorts is not None:
        attempts = attempts.where(F.col("cohort_date").isin(list(cohorts)))

    approved = F.col("is_approved")
    zero = F.lit(0).cast("decimal(38,2)")
    return attempts.groupBy("cohort_date", "payment_method").agg(
        F.max(F.col("is_chargeback_eligible").cast("int")).cast("boolean").alias(
            "is_chargeback_eligible"
        ),
        F.count("*").alias("attempt_count"),
        F.sum(approved.cast("int")).alias("approved_count"),
        F.coalesce(F.sum(F.when(approved, F.col("amount"))), zero).alias("approved_amount"),
        F.sum((approved & F.col("has_chargeback")).cast("int")).alias(
            "chargeback_transaction_count"
        ),
        F.sum((approved & F.col("has_fraud_chargeback")).cast("int")).alias(
            "fraud_chargeback_transaction_count"
        ),
        F.coalesce(F.sum(F.when(approved, F.col("chargeback_amount"))), zero).alias(
            "chargeback_amount"
        ),
        F.coalesce(F.sum(F.when(approved, F.col("fraud_chargeback_amount"))), zero).alias(
            "fraud_chargeback_amount"
        ),
        F.datediff(F.lit(as_of_date), F.first("cohort_date")).alias("cohort_age_days"),
        (F.datediff(F.lit(as_of_date), F.first("cohort_date")) >= MATURITY_WINDOW_DAYS).alias(
            "is_mature"
        ),
        F.max("_updated_at").alias("_updated_at"),
    )


# --------------------------------------------------------------------------------------
# Incremental (chaves e safras afetadas)
# --------------------------------------------------------------------------------------
def current_watermarks(
    transactions: DataFrame, evaluations: DataFrame, chargebacks: DataFrame
) -> Watermarks:
    epoch = datetime(1900, 1, 1)

    def _max(df: DataFrame) -> datetime:
        value = df.agg(F.max("_ingested_at")).first()[0]
        return value or epoch

    return Watermarks(_max(transactions), _max(evaluations), _max(chargebacks))


def affected_transaction_ids(
    transactions: DataFrame,
    evaluations: DataFrame,
    chargebacks: DataFrame,
    since: Watermarks,
) -> DataFrame:
    """Toda transação que recebeu evento novo em QUALQUER fonte desde o último run.

    Chargeback de D+120 reabre a transação de D-120: sem janela fixa, sem perda.
    """
    return (
        transactions.where(F.col("_ingested_at") > F.lit(since.transactions))
        .select("transaction_id")
        .unionByName(
            evaluations.where(F.col("_ingested_at") > F.lit(since.evaluations)).select(
                "transaction_id"
            )
        )
        .unionByName(
            chargebacks.where(F.col("_ingested_at") > F.lit(since.chargebacks)).select(
                "transaction_id"
            )
        )
        .distinct()
    )


def merge_by_key(current: DataFrame, changes: DataFrame, keys: list[str]) -> DataFrame:
    """Equivalente em memória ao MERGE (delete+insert por chave). Em Delta: io_delta.merge."""
    return current.join(changes.select(*keys), keys, "left_anti").unionByName(changes)


def affected_cohorts(changed_attempts: DataFrame) -> list[date]:
    return [r.cohort_date for r in changed_attempts.select("cohort_date").distinct().collect()]
