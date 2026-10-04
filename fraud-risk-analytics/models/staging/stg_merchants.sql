-- Grão: 1 linha por lojista.
select
    trim(merchant_id) as merchant_id,
    trim(mcc) as mcc,
    lower(trim(segment)) as segment,
    try_cast(created_at as timestamp) as merchant_created_at,
    _ingested_at
from {{ source('raw_v2', 'raw_merchants') }}
qualify row_number() over (partition by merchant_id order by _ingested_at desc) = 1
