-- Grão: 1 linha por transaction_id (dedup técnico de reentrega da ingestão).
-- Duplicidade de NEGÓCIO (ids diferentes, mesmo pagamento) NÃO é removida aqui:
-- é sinalizada em int_transactions__duplicate_suspects para não esconder cobrança em dobro.
with source as (
    select * from {{ source('raw', 'raw_transactions') }}
),

typed as (
    select
        trim(transaction_id) as transaction_id,
        trim(user_id) as user_id,
        -- Contrato: timestamps em America/Sao_Paulo. Safra = data local.
        try_cast(transaction_at as timestamp) as transaction_at,
        cast(try_cast(transaction_at as timestamp) as date) as transaction_date,
        try_cast(amount as decimal(18, 2)) as amount,
        lower(trim(status)) as status,
        lower(trim(payment_method)) as payment_method,
        _ingested_at
    from source
),

deduped as (
    select *
    from typed
    qualify row_number() over (
        partition by transaction_id
        order by _ingested_at desc
    ) = 1
)

select
    *,
    status = 'approved' as is_approved,
    payment_method in (
        {%- for m in var('chargeback_eligible_methods') %}'{{ m }}'{% if not loop.last %}, {% endif %}
        {% endfor -%}
    ) as is_chargeback_eligible
from deduped
