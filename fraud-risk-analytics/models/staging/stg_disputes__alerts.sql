-- Grão: 1 linha por alerta pré-disputa (RDR, Ethoca). Resolvido antes de virar chargeback.
select
    trim(alert_id) as alert_id,
    trim(transaction_id) as transaction_id,
    lower(trim(alert_provider)) as alert_provider,
    try_cast(alerted_at as timestamp) as alerted_at,
    lower(trim(alert_type)) as alert_type,
    lower(trim(action_taken)) as action_taken,
    _ingested_at
from {{ source('raw_v2', 'raw_dispute_alerts') }}
qualify row_number() over (partition by alert_id order by _ingested_at desc) = 1
