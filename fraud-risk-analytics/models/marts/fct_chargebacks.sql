-- Grão: 1 linha por evento de chargeback. Base da Perda Bruta e da curva de maturação.
with chargebacks as (
    select * from {{ ref('stg_acquirer__chargebacks') }}
),

transactions as (
    select * from {{ ref('stg_payments__transactions') }}
)

select
    chargebacks.chargeback_id,
    chargebacks.transaction_id,
    chargebacks.chargeback_at,
    cast(chargebacks.chargeback_at as date) as chargeback_date,
    transactions.transaction_date as cohort_date,
    transactions.payment_method,
    transactions.status as transaction_status,
    transactions.amount as transaction_amount,
    chargebacks.reason_code_raw,
    chargebacks.network_reason_code,
    chargebacks.reason_category,
    chargebacks.is_fraud_reason,
    chargebacks.cb_amount,
    datediff('day', transactions.transaction_at, chargebacks.chargeback_at) as days_to_chargeback,
    coalesce(
        datediff('day', transactions.transaction_at, chargebacks.chargeback_at)
        > {{ var('maturity_window_days') }},
        false
    ) as is_beyond_maturity_window,
    -- anomalias (sinalizadas, nunca filtradas)
    transactions.transaction_id is null as is_orphan,
    coalesce(transactions.status <> 'approved', true) as is_on_non_approved_transaction,
    coalesce(chargebacks.cb_amount > transactions.amount, false) as is_amount_above_transaction,
    chargebacks._ingested_at
from chargebacks
left join transactions on chargebacks.transaction_id = transactions.transaction_id
