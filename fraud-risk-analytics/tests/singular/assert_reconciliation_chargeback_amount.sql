-- Reconciliação ponta a ponta: nenhum centavo de chargeback se perde entre Bronze e Gold.
-- Diferença aqui = bug de pipeline (join que duplica ou descarta). Bloqueia publicação.
with raw as (
    select coalesce(sum(try_cast(cb_amount as decimal(18, 2))), 0) as amount
    from {{ source('raw', 'raw_chargebacks') }}
),

events as (
    select coalesce(sum(cb_amount), 0) as amount from {{ ref('fct_chargebacks') }}
),

attempts as (
    select coalesce(sum(chargeback_amount), 0) as amount from {{ ref('fct_payment_attempts') }}
),

orphans as (
    select coalesce(sum(cb_amount), 0) as amount from {{ ref('fct_chargebacks') }}
    where is_orphan
)

select
    raw.amount as raw_amount,
    events.amount as events_amount,
    attempts.amount as attempts_amount
from raw, events, attempts, orphans
where
    raw.amount <> events.amount
    or events.amount <> attempts.amount + orphans.amount
