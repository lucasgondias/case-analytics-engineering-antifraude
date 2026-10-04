{{ config(materialized='view') }}
-- Exposição por lojista (calculada na leitura: depende de "hoje").
-- O chargeback é debitado do saldo do lojista, mas a plataforma repassa o dinheiro em D+2/D+14/D+30,
-- antes de a janela de contestação (até 120 dias) fechar. Valor já repassado e ainda contestável
-- é o que a plataforma perde se o lojista não tiver saldo para cobrir (saldo negativo não pago).
-- Contrato v2 trará o ledger (saldo disponível, lançamentos futuros, crédito em aberto) para
-- descontar o que já está garantido.
{%- set dispute_window_days = 120 %}

with card_sales as (
    select
        merchant_id,
        amount,
        chargeback_amount,
        {{ dbt.datediff("cohort_date", as_of_date(), "day") }} as age_days,
        coalesce(payout_term_days, 30) as payout_term_days
    from {{ ref('fct_payment_attempts') }}
    where is_approved and is_chargeback_eligible and merchant_id is not null
),

historical_rate as (
    -- taxa de chargeback em valor das safras já fechadas do lojista (fallback: carteira toda)
    select
        merchant_id,
        sum(chargeback_amount) / nullif(sum(amount), 0) as merchant_cb_rate
    from card_sales
    where age_days >= {{ dispute_window_days }}
    group by merchant_id
),

portfolio_rate as (
    select sum(chargeback_amount) / nullif(sum(amount), 0) as portfolio_cb_rate
    from card_sales
    where age_days >= {{ dispute_window_days }}
),

open_window as (
    select
        merchant_id,
        sum(amount) filter (where age_days < {{ dispute_window_days }}) as open_window_amount,
        sum(amount) filter (
            where age_days < {{ dispute_window_days }} and age_days >= payout_term_days
        ) as released_open_window_amount,
        sum(chargeback_amount) filter (where age_days < {{ dispute_window_days }})
            as open_window_chargeback_amount
    from card_sales
    group by merchant_id
)

select
    open_window.merchant_id,
    coalesce(open_window.open_window_amount, 0) as open_window_amount,
    coalesce(open_window.released_open_window_amount, 0) as released_open_window_amount,
    coalesce(open_window.open_window_chargeback_amount, 0) as open_window_chargeback_amount,
    coalesce(historical_rate.merchant_cb_rate, portfolio_rate.portfolio_cb_rate) as expected_cb_rate,
    coalesce(open_window.released_open_window_amount, 0)
    * coalesce(historical_rate.merchant_cb_rate, portfolio_rate.portfolio_cb_rate, 0)
        as expected_chargeback_on_released
from open_window
cross join portfolio_rate
left join historical_rate on open_window.merchant_id = historical_rate.merchant_id
