-- Grão: lojista x mês de notificação. Numa plataforma, o chargeback é debitado do saldo do
-- LOJISTA; a perda da plataforma é o que não consegue recuperar dele (saldo insuficiente,
-- lojista inativo). Concentração de perda em poucos lojistas é sinal de fraude de lojista.
with chargebacks as (
    select * from {{ ref('fct_chargebacks') }}
    where merchant_id is not null
),

med as (
    select
        enrichment.merchant_id,
        cast(date_trunc('month', med_claims.claimed_at) as date) as month_start,
        sum(med_claims.claim_amount) as pix_med_claim_amount,
        sum(med_claims.returned_amount) as pix_med_returned_amount
    from {{ ref('stg_pix__med_claims') }} as med_claims
    inner join {{ ref('stg_payments__enrichment') }} as enrichment
        on med_claims.transaction_id = enrichment.transaction_id
    group by 1, 2
),

cb as (
    select
        merchant_id,
        cast(date_trunc('month', chargeback_at) as date) as month_start,
        count(*) as chargeback_count,
        sum(cb_amount) as gross_chargeback_amount,
        sum(cb_amount) filter (where is_fraud_reason) as gross_fraud_chargeback_amount,
        sum(net_loss_amount) as net_chargeback_loss_amount,
        sum(recovered_from_merchant_amount) as recovered_from_merchant_amount,
        sum(platform_unrecovered_amount) as platform_unrecovered_amount
    from chargebacks
    group by 1, 2
),

merchant_months as (
    select
        merchant_id,
        month_start
    from cb
    union
    select
        merchant_id,
        month_start
    from med
)

select
    merchant_months.merchant_id,
    merchants.segment,
    merchants.mcc,
    {{ dbt.datediff("merchants.merchant_created_at", "merchant_months.month_start", "day") }} as merchant_age_days,
    merchant_months.month_start,
    coalesce(cb.chargeback_count, 0) as chargeback_count,
    coalesce(cb.gross_chargeback_amount, 0) as gross_chargeback_amount,
    coalesce(cb.gross_fraud_chargeback_amount, 0) as gross_fraud_chargeback_amount,
    coalesce(cb.net_chargeback_loss_amount, 0) as net_chargeback_loss_amount,
    coalesce(cb.recovered_from_merchant_amount, 0) as recovered_from_merchant_amount,
    coalesce(cb.platform_unrecovered_amount, 0) as platform_unrecovered_amount,
    coalesce(med.pix_med_claim_amount, 0) as pix_med_claim_amount,
    coalesce(med.pix_med_claim_amount, 0) - coalesce(med.pix_med_returned_amount, 0)
        as pix_med_net_loss_amount
from merchant_months
left join cb
    on merchant_months.merchant_id = cb.merchant_id and merchant_months.month_start = cb.month_start
left join med
    on merchant_months.merchant_id = med.merchant_id and merchant_months.month_start = med.month_start
left join {{ ref('stg_merchants') }} as merchants
    on merchant_months.merchant_id = merchants.merchant_id
