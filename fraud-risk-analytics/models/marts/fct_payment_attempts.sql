{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='transaction_id',
        on_schema_change='append_new_columns'
    )
}}
{#-
    Fontes que podem alterar uma transação já processada. Cada uma tem seu watermark gravado no
    próprio fato. Evento novo em QUALQUER uma reabre a transação.
-#}
{%- set event_sources = [
    ('stg_payments__transactions', '_tx_watermark'),
    ('stg_risk__evaluations', '_eval_watermark'),
    ('stg_acquirer__chargebacks', '_cb_watermark'),
    ('stg_payments__enrichment', '_enrichment_watermark'),
    ('fct_fraud_labels', '_label_watermark'),
    ('stg_payments__refunds', '_refund_watermark'),
] -%}
-- Grão: 1 linha por tentativa de pagamento (transaction_id). Fonte única para aprovação,
-- decisão de risco, desfecho (chargeback, rótulo de fraude, reembolso) e flags de qualidade.
--
-- Incremental por CHAVES AFETADAS com sobreposição (macro incremental_since):
--   * evento novo em qualquer fonte reabre a transação (chargeback D+120 reabre a safra certa);
--   * a janela de sobreposição cobre evento ingerido fora de ordem;
--   * transações vizinhas do mesmo usuário (janela de duplicidade) também reabrem, para a flag
--     de duplicidade da transação ANTIGA não ficar desatualizada.

with
{% if is_incremental() %}
    changed_keys as (
        {%- for model_name, watermark in event_sources %}
            select transaction_id from {{ ref(model_name) }}
            where _ingested_at > {{ incremental_since(watermark) }}
            {%- if not loop.last %}
                union
            {%- endif %}
        {%- endfor %}
    ),

    neighbor_keys as (
    -- B4: duplicata nova também reabre a transação antiga do mesmo usuário.
        select older.transaction_id
        from {{ ref('stg_payments__transactions') }} as newer
        inner join {{ ref('stg_payments__transactions') }} as older
            on
                newer.user_id = older.user_id
                and abs(datediff('second', newer.transaction_at, older.transaction_at))
                <= {{ var('duplicate_window_seconds') }}
        where newer.transaction_id in (select ck.transaction_id from changed_keys as ck)
    ),

    affected_keys as (
        select transaction_id from changed_keys
        union
        select transaction_id from neighbor_keys
    ),
{% endif %}

transactions as (
    select * from {{ ref('stg_payments__transactions') }}
    {% if is_incremental() %}
        where transaction_id in (select ak.transaction_id from affected_keys as ak)
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

enrichment as (
    select * from {{ ref('stg_payments__enrichment') }}
),

issuer_codes as (
    select * from {{ ref('ref_issuer_response_codes') }}
),

fraud_labels as (
    select * from {{ ref('int_fraud_labels__per_transaction') }}
),

refunds as (
    select
        transaction_id,
        sum(refund_amount) as refund_amount,
        sum(refund_amount) filter (where is_fraud_refund) as fraud_refund_amount
    from {{ ref('stg_payments__refunds') }}
    group by transaction_id
),

watermarks as (
    select
        {%- for model_name, watermark in event_sources %}
            (
                select coalesce(max(_ingested_at), timestamp '1900-01-01')
                from {{ ref(model_name) }}
            ) as {{ watermark }}{% if not loop.last %},{% endif %}
        {%- endfor %}
)

select
    -- chaves e atributos
    transactions.transaction_id,
    transactions.user_id,
    enrichment.merchant_id,
    enrichment.order_id,
    enrichment.country,
    transactions.transaction_at,
    transactions.transaction_date as cohort_date,
    transactions.amount,
    enrichment.currency,
    transactions.status,
    transactions.payment_method,
    enrichment.card_network,
    transactions.is_chargeback_eligible,
    enrichment.three_ds_result,
    enrichment.three_ds_result = 'authenticated' as is_3ds_authenticated,

    -- funil
    transactions.is_approved,
    case
        when transactions.status = 'approved' then null
        when transactions.status = 'error' then 'technical_error'
        -- B3: recusa humana não é recusa do motor nem do emissor
        when risk.risk_action = 'reject' and risk.decision_source in ('manual_review', 'partner_review')
            then 'manual_review'
        when risk.risk_action = 'reject' then 'risk_engine'
        when risk.risk_action = 'review' then 'manual_review'
        when risk.transaction_id is null then 'unknown_no_risk_evaluation'
        else 'issuer_or_acquirer'
    end as decline_source,
    case
        when transactions.status <> 'declined' then null
        when risk.risk_action in ('reject', 'review') then null
        else coalesce(issuer_codes.decline_type, 'unknown')
    end as issuer_decline_type,
    enrichment.issuer_response_code,
    enrichment.capture_status,
    enrichment.settled_at,
    enrichment.payout_term_days,
    coalesce(enrichment.capture_status = 'captured', false) as is_settled,

    -- decisão de risco vigente na autorização
    risk.evaluation_id,
    risk.risk_score,
    risk.risk_action,
    risk.rule_triggered,
    risk.decision_source,
    coalesce(risk.evaluation_count, 0) as evaluation_count,

    -- desfecho: chargeback (label tardio)
    chargebacks.transaction_id is not null as has_chargeback,
    coalesce(chargebacks.has_fraud_chargeback, false) as has_fraud_chargeback,
    coalesce(chargebacks.chargeback_count, 0) as chargeback_count,
    coalesce(chargebacks.chargeback_amount, 0) as chargeback_amount,
    coalesce(chargebacks.fraud_chargeback_amount, 0) as fraud_chargeback_amount,
    chargebacks.first_chargeback_at,
    datediff('day', transactions.transaction_at, chargebacks.first_chargeback_at)
        as days_to_first_chargeback,

    -- desfecho: rótulo de fraude (primeira evidência de qualquer fonte)
    fraud_labels.transaction_id is not null as is_fraud_labeled,
    fraud_labels.first_fraud_label_at,
    fraud_labels.first_fraud_label_source,
    fraud_labels.fraud_label_sources,
    datediff('day', transactions.transaction_at, fraud_labels.first_fraud_label_at)
        as days_to_fraud_label,

    -- desfecho: reembolso
    coalesce(refunds.refund_amount, 0) as refund_amount,
    coalesce(refunds.fraud_refund_amount, 0) as fraud_refund_amount,

    -- flags de qualidade (erros silenciosos): nunca filtram, só sinalizam
    transactions.is_approved and risk.transaction_id is null
        as is_approved_without_risk_evaluation,
    coalesce(transactions.is_approved and risk.risk_action = 'reject', false)
        as is_approved_despite_risk_reject,
    coalesce(duplicates.is_duplicate_candidate, false) as is_duplicate_candidate,
    duplicates.first_transaction_id_in_group as duplicate_of_transaction_id,

    -- metadados de pipeline
    {%- for model_name, watermark in event_sources %}
        watermarks.{{ watermark }},
    {%- endfor %}
    current_timestamp as _updated_at
from transactions
cross join watermarks
left join risk on transactions.transaction_id = risk.transaction_id
left join chargebacks on transactions.transaction_id = chargebacks.transaction_id
left join duplicates on transactions.transaction_id = duplicates.transaction_id
left join enrichment on transactions.transaction_id = enrichment.transaction_id
left join issuer_codes on enrichment.issuer_response_code = issuer_codes.issuer_response_code
left join fraud_labels on transactions.transaction_id = fraud_labels.transaction_id
left join refunds on transactions.transaction_id = refunds.transaction_id
