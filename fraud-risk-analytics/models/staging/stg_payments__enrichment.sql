-- Grão: 1 linha por transaction_id. Atributos do contrato v2 (fonte proposta, sem dados no case).
with source as (
    select * from {{ source('raw_v2', 'raw_payment_enrichment') }}
)

select
    trim(transaction_id) as transaction_id,
    trim(merchant_id) as merchant_id,
    trim(order_id) as order_id,
    -- operação multi-país: moeda, fuso da safra e prazos de disputa diferem por país
    upper(trim(country)) as country,
    nullif(lower(trim(card_network)), '') as card_network,
    nullif(trim(card_fingerprint), '') as card_fingerprint,
    nullif(trim(bin), '') as bin,
    nullif(lower(trim(three_ds_result)), '') as three_ds_result,
    nullif(trim(issuer_response_code), '') as issuer_response_code,
    lower(trim(capture_status)) as capture_status,
    try_cast(settled_at as timestamp) as settled_at,
    -- prazo de repasse ao lojista (D+2/D+14/D+30): repasse antes da janela de chargeback = exposição
    try_cast(payout_term_days as integer) as payout_term_days,
    upper(trim(currency)) as currency,
    _ingested_at
from source
qualify row_number() over (partition by transaction_id order by _ingested_at desc) = 1
