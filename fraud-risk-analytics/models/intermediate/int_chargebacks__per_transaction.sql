-- Grão: 1 linha por transaction_id que recebeu ao menos 1 chargeback.
select
    transaction_id,
    count(*) as chargeback_count,
    sum(cb_amount) as chargeback_amount,
    sum(cb_amount) filter (where is_fraud_reason) as fraud_chargeback_amount,
    count(*) filter (where is_fraud_reason) > 0 as has_fraud_chargeback,
    min(chargeback_at) as first_chargeback_at,
    max(chargeback_at) as last_chargeback_at,
    max(_ingested_at) as last_chargeback_ingested_at
from {{ ref('stg_acquirer__chargebacks') }}
group by transaction_id
