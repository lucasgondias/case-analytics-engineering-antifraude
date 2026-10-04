"""Job PySpark do Lakeflow Jobs: variáveis de velocidade por cliente.

Uso (spark_python_task): run_velocity_features.py <catalogo>
Lê o Bronze publicado pelo pipeline do Lakeflow (<catalogo>.raw.raw_transactions), recalcula as
últimas 24 horas com 48 horas de histórico e faz MERGE em <catalogo>.features.user_velocity.
Definição para Databricks; não é executada no protótipo local. A regra é testada em tests/.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from fraud_risk_spark import transformations as T  # noqa: E402
from fraud_risk_spark.io_delta import merge_velocity_features  # noqa: E402


def main(catalog: str) -> None:
    spark = SparkSession.builder.getOrCreate()
    raw = spark.table(f"{catalog}.raw.raw_transactions").withColumn(
        "_ingested_at", F.date_format("_ingested_at", "yyyy-MM-dd HH:mm:ss")
    )
    recent = T.stg_transactions(raw).where(
        F.col("transaction_at") >= F.expr("current_timestamp() - INTERVAL 48 HOURS")
    )
    features = T.velocity_features(recent).where(
        F.col("transaction_at") >= F.expr("current_timestamp() - INTERVAL 24 HOURS")
    )
    merge_velocity_features(spark, features, f"{catalog}.features.user_velocity")


if __name__ == "__main__":
    main(sys.argv[1])
