{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='transaction_id',
        on_schema_change='append_new_columns'
    )
}}
-- Grão: 1 linha por tentativa de pagamento (transaction_id). Fonte única para aprovação,
-- decisão de risco e desfecho de chargeback.
--
-- Incremental por CHAVES AFETADAS (não por janela fixa):
-- reprocessa toda transação que recebeu evento novo em qualquer das 3 fontes desde o
-- último watermark. Um chargeback de D+120 reabre a transação de D-120 -> a safra correta
-- é atualizada sem reprocessar 90 dias inteiros e sem perder CB fora da janela.

with transactions as (
    select * from {{ ref('stg_payments__transactions') }}
    {% if is_incremental() %}
        where transaction_id in (
            select transaction_id from {{ ref('stg_payments__transactions') }}
            where _ingested_at > (select max(_tx_watermark) from {{ this }})
            union
            select transaction_id from {{ ref('stg_risk__evaluations') }}
            where _ingested_at > (select max(_eval_watermark) from {{ this }})
            union
            select transaction_id from {{ ref('stg_acquirer__chargebacks') }}
            where _ingested_at > (select max(_cb_watermark) from {{ this }})
        )
    {% endif %}
),

risk as (
    select * from {{ ref('int_risk__decision_per_transaction') }}
),

chargebacks as (
    select * from {{ ref('int_chargebacks__per_transaction') }}
),

duplicates as (
    select * from {{ ref('int_transactions__duplicate_suspects') }}
),

watermarks as (
    -- Watermark global por fonte, gravado em toda linha para o próximo run incremental.
    select
        (select max(_ingested_at) from {{ ref('stg_payments__transactions') }}) as _tx_watermark,
        (select max(_ingested_at) from {{ ref('stg_risk__evaluations') }}) as _eval_watermark,
        (
            select coalesce(max(_ingested_at), timestamp '1900-01-01')
            from {{ ref('stg_acquirer__chargebacks') }}
        ) as _cb_watermark
)

select
    -- chaves e atributos
    transactions.transaction_id,
    transactions.user_id,
    transactions.transaction_at,
    transactions.transaction_date as cohort_date,
    transactions.amount,
    transactions.status,
    transactions.payment_method,
    transactions.is_chargeback_eligible,

    -- funil
    transactions.is_approved,
    case
        when transactions.status = 'approved' then null
        when transactions.status = 'error' then 'technical_error'
        when risk.risk_action = 'reject' then 'risk_engine'
        when risk.transaction_id is null then 'unknown_no_risk_evaluation'
        else 'issuer_or_acquirer'
    end as decline_source,

    -- decisão de risco vigente na autorização
    risk.evaluation_id,
    risk.risk_score,
    risk.risk_action,
    risk.rule_triggered,
    risk.decision_source,
    coalesce(risk.evaluation_count, 0) as evaluation_count,

    -- desfecho (label tardio)
    chargebacks.transaction_id is not null as has_chargeback,
    coalesce(chargebacks.has_fraud_chargeback, false) as has_fraud_chargeback,
    coalesce(chargebacks.chargeback_count, 0) as chargeback_count,
    coalesce(chargebacks.chargeback_amount, 0) as chargeback_amount,
    coalesce(chargebacks.fraud_chargeback_amount, 0) as fraud_chargeback_amount,
    chargebacks.first_chargeback_at,
    datediff('day', transactions.transaction_at, chargebacks.first_chargeback_at)
        as days_to_first_chargeback,

    -- flags de qualidade (erros silenciosos): nunca filtram, só sinalizam
    transactions.is_approved and risk.transaction_id is null
        as is_approved_without_risk_evaluation,
    coalesce(transactions.is_approved and risk.risk_action = 'reject', false)
        as is_approved_despite_risk_reject,
    coalesce(duplicates.is_duplicate_candidate, false) as is_duplicate_candidate,
    duplicates.first_transaction_id_in_group as duplicate_of_transaction_id,

    -- metadados de pipeline
    watermarks._tx_watermark,
    watermarks._eval_watermark,
    watermarks._cb_watermark,
    current_timestamp as _updated_at
from transactions
cross join watermarks
left join risk on transactions.transaction_id = risk.transaction_id
left join chargebacks on transactions.transaction_id = chargebacks.transaction_id
left join duplicates on transactions.transaction_id = duplicates.transaction_id
