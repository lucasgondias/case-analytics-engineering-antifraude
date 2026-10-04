-- Grão: 1 linha por chargeback com o desfecho mais recente da disputa.
select
    trim(chargeback_id) as chargeback_id,
    lower(trim(dispute_stage)) as dispute_stage,
    lower(trim(outcome)) as outcome,
    try_cast(resolved_at as timestamp) as resolved_at,
    coalesce(try_cast(recovered_amount as decimal(18, 2)), 0) as recovered_amount,
    coalesce(try_cast(recovered_from_merchant_amount as decimal(18, 2)), 0)
        as recovered_from_merchant_amount,
    _ingested_at
from {{ source('raw_v2', 'raw_chargeback_outcomes') }}
qualify row_number() over (partition by chargeback_id order by _ingested_at desc) = 1
