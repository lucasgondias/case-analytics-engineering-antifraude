-- Grão: 1 linha por chargeback_id (evento). Uma transação pode ter N chargebacks
-- (parcial, re-apresentação, pré-arbitragem).
with source as (
    select * from {{ source('raw', 'raw_chargebacks') }}
),

typed as (
    select
        trim(chargeback_id) as chargeback_id,
        trim(transaction_id) as transaction_id,
        try_cast(chargeback_at as timestamp) as chargeback_at,
        trim(reason_code) as reason_code_raw,
        -- '10.4_fraud' -> '10.4' (código da bandeira). Sufixo textual não é confiável.
        regexp_extract(trim(reason_code), '^([0-9]+(\.[0-9]+)?)', 1) as network_reason_code,
        try_cast(cb_amount as decimal(18, 2)) as cb_amount,
        _ingested_at
    from source
),

reason_codes as (
    select * from {{ ref('ref_chargeback_reason_codes') }}
)

select
    typed.*,
    coalesce(reason_codes.reason_category, 'unmapped') as reason_category,
    coalesce(reason_codes.reason_category, 'unmapped') = 'fraud' as is_fraud_reason
from typed
left join reason_codes
    on typed.network_reason_code = reason_codes.reason_code
qualify row_number() over (partition by typed.chargeback_id order by typed._ingested_at desc) = 1
