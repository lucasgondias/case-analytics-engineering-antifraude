-- Grão: 1 linha por contestação MED 2.0. Perda = valor contestado - valor efetivamente devolvido.
select
    trim(claim_id) as claim_id,
    trim(transaction_id) as transaction_id,
    try_cast(claimed_at as timestamp) as claimed_at,
    try_cast(claim_amount as decimal(18, 2)) as claim_amount,
    lower(trim(claim_status)) as claim_status,
    coalesce(try_cast(returned_amount as decimal(18, 2)), 0) as returned_amount,
    _ingested_at
from {{ source('raw_v2', 'raw_pix_med_claims') }}
qualify row_number() over (partition by claim_id order by _ingested_at desc) = 1
