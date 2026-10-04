-- Grão: 1 linha por relatório TC40/SAFE.
select
    trim(report_id) as report_id,
    trim(transaction_id) as transaction_id,
    lower(trim(card_network)) as card_network,
    try_cast(reported_at as timestamp) as reported_at,
    lower(trim(fraud_type)) as fraud_type,
    _ingested_at
from {{ source('raw_v2', 'raw_issuer_fraud_reports') }}
qualify row_number() over (partition by report_id order by _ingested_at desc) = 1
