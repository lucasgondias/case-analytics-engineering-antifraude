"""SparkSession local para o protótipo e para os testes."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


def local_session(app_name: str = "fraud-risk-analytics") -> SparkSession:
    # Garante que driver e workers usem o mesmo Python (no Windows o PATH costuma divergir).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
