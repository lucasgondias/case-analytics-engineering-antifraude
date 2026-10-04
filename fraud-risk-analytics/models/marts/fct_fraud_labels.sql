-- Grão: 1 linha por EVIDÊNCIA de fraude (label store).
-- Separar evidências do fato de transação permite: rótulo antecipado (TC40/SAFE antes do chargeback),
-- auditoria da origem do rótulo e treino de modelo sem vazamento (label_at = quando ficamos sabendo).
with issuer_reports as (
    select
        transaction_id,
        report_id as evidence_id,
        'issuer_fraud_report' as label_source,
        reported_at as label_at,
        cast(null as decimal(18, 2)) as evidence_amount,
        _ingested_at
    from {{ ref('stg_network__issuer_fraud_reports') }}
),

fraud_chargebacks as (
    select
        transaction_id,
        chargeback_id as evidence_id,
        'chargeback_fraud' as label_source,
        chargeback_at as label_at,
        cb_amount as evidence_amount,
        _ingested_at
    from {{ ref('stg_acquirer__chargebacks') }}
    where is_fraud_reason
),

fraud_refunds as (
    select
        transaction_id,
        refund_id as evidence_id,
        'fraud_refund' as label_source,
        refunded_at as label_at,
        refund_amount as evidence_amount,
        _ingested_at
    from {{ ref('stg_payments__refunds') }}
    where is_fraud_refund
),

dispute_alerts as (
    select
        transaction_id,
        alert_id as evidence_id,
        'dispute_alert' as label_source,
        alerted_at as label_at,
        cast(null as decimal(18, 2)) as evidence_amount,
        _ingested_at
    from {{ ref('stg_disputes__alerts') }}
    where alert_type like '%fraud%'
),

pix_med as (
    select
        transaction_id,
        claim_id as evidence_id,
        'pix_med_claim' as label_source,
        claimed_at as label_at,
        claim_amount as evidence_amount,
        _ingested_at
    from {{ ref('stg_pix__med_claims') }}
)

select * from issuer_reports
union all
select * from fraud_chargebacks
union all
select * from fraud_refunds
union all
select * from dispute_alerts
union all
select * from pix_med
