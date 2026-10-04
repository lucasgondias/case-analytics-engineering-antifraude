-- Grão: 1 linha por reembolso. refund_reason distingue reembolso por fraude/alerta de pedido do cliente.
select
    trim(refund_id) as refund_id,
    trim(transaction_id) as transaction_id,
    try_cast(refunded_at as timestamp) as refunded_at,
    try_cast(refund_amount as decimal(18, 2)) as refund_amount,
    lower(trim(refund_reason)) as refund_reason,
    lower(trim(refund_reason)) in ('fraud_alert', 'fraud_confirmed', 'dispute_alert') as is_fraud_refund,
    _ingested_at
from {{ source('raw_v2', 'raw_refunds') }}
qualify row_number() over (partition by refund_id order by _ingested_at desc) = 1
