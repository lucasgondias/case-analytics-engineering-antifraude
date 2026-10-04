"""Camada de I/O para Delta Lake (Databricks / Spark com delta-spark).

Separada das transformações para que a regra rode e seja testada em memória (tests/), e a
escrita use as primitivas transacionais do Delta em produção. Não é executada no protótipo
local (Delta no Windows exige Hadoop nativo); a lógica que ela grava é a mesma testada.
"""

from __future__ import annotations

from datetime import date
from typing import Iterable

from pyspark.sql import DataFrame


def merge_payment_attempts(spark, changes: DataFrame, table: str) -> None:
    """MERGE por transaction_id. Só as chaves afetadas chegam em `changes`."""
    from delta.tables import DeltaTable  # import tardio: dependência só em produção

    target = DeltaTable.forName(spark, table)
    (
        target.alias("t")
        .merge(changes.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def replace_cohorts(cohort_aggregate: DataFrame, table: str, cohorts: Iterable[date]) -> None:
    """Sobrescreve atomicamente só as partições de safra afetadas (replaceWhere)."""
    cohort_list = sorted(set(cohorts))
    if not cohort_list:
        return
    predicate = "cohort_date IN ({})".format(", ".join(f"DATE'{c.isoformat()}'" for c in cohort_list))
    (
        cohort_aggregate.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(table)
    )
