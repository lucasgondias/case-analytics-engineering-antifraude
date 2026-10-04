-- Grão: 1 linha por evento de chargeback. Base da Perda Bruta/Líquida, do ratio das bandeiras
-- (por data de notificação) e da curva de maturação.
with chargebacks as (
    select * from {{ ref('stg_acquirer__chargebacks') }}
),

transactions as (
    select * from {{ ref('stg_payments__transactions') }}
),

enrichment as (
    select * from {{ ref('stg_payments__enrichment') }}
),

outcomes as (
    select * from {{ ref('stg_acquirer__chargeback_outcomes') }}
)

select
    chargebacks.chargeback_id,
    chargebacks.transaction_id,
    enrichment.merchant_id,
    enrichment.card_network,
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
    -- liability shift: fraude em transação autenticada (3DS) tende a ser do emissor
    coalesce(enrichment.three_ds_result = 'authenticated', false) as is_3ds_authenticated,

    -- disputa e perda líquida
    coalesce(outcomes.dispute_stage, 'first_chargeback') as dispute_stage,
    coalesce(outcomes.outcome, 'open') as dispute_outcome,
    outcomes.resolved_at,
    coalesce(outcomes.recovered_amount, 0) as recovered_in_dispute_amount,
    -- plataforma debita o lojista; perda da plataforma = o que não conseguiu recuperar dele
    coalesce(outcomes.recovered_from_merchant_amount, 0) as recovered_from_merchant_amount,
    chargebacks.cb_amount - coalesce(outcomes.recovered_amount, 0) as net_loss_amount,
    greatest(
        chargebacks.cb_amount
        - coalesce(outcomes.recovered_amount, 0)
        - coalesce(outcomes.recovered_from_merchant_amount, 0),
        0
    ) as platform_unrecovered_amount,

    -- anomalias (sinalizadas, nunca filtradas)
    transactions.transaction_id is null as is_orphan,
    coalesce(transactions.status <> 'approved', true) as is_on_non_approved_transaction,
    coalesce(chargebacks.cb_amount > transactions.amount, false) as is_amount_above_transaction,
    chargebacks._ingested_at
from chargebacks
left join transactions on chargebacks.transaction_id = transactions.transaction_id
left join enrichment on chargebacks.transaction_id = enrichment.transaction_id
left join outcomes on chargebacks.chargeback_id = outcomes.chargeback_id
