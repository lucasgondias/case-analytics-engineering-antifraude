{{ config(materialized='view') }}
-- Safra com idade e maturidade calculadas NA LEITURA (B2). View, nunca tabela incremental:
-- o que depende de "hoje" não pode ser congelado no momento em que a safra foi processada.
select
    agg.*,
    {{ dbt.datediff("agg.cohort_date", as_of_date(), "day") }} as cohort_age_days,
    {{ dbt.datediff("agg.cohort_date", as_of_date(), "day") }} >= {{ var('maturity_window_days') }} as is_mature,
    agg.chargeback_amount / nullif(agg.approved_amount, 0) as chargeback_rate_amount,
    agg.chargeback_transaction_count / nullif(agg.approved_count, 0) as chargeback_rate_qty,
    -- fraude em bps do valor aprovado (linguagem padrão do setor), com todas as evidências
    10000.0 * agg.fraud_labeled_amount / nullif(agg.approved_amount, 0) as fraud_bps
from {{ ref('agg_chargeback_cohort_daily') }} as agg
