-- Curva de maturação: % acumulado do valor de chargeback por dias desde a transação.
-- Serve para (1) validar empiricamente a janela de 90 dias acordada com Risco e
-- (2) projetar a perda final de safras imaturas (CB observado / % maturado esperado).
with chargebacks as (
    select * from {{ ref('fct_chargebacks') }}
    where not is_orphan
),

buckets as (
    select
        case
            when days_to_chargeback <= 15 then 15
            when days_to_chargeback <= 30 then 30
            when days_to_chargeback <= 45 then 45
            when days_to_chargeback <= 60 then 60
            when days_to_chargeback <= 90 then 90
            when days_to_chargeback <= 120 then 120
            else 999
        end as days_bucket,
        sum(cb_amount) as cb_amount,
        count(*) as cb_count
    from chargebacks
    group by 1
)

select
    days_bucket,
    cb_count,
    cb_amount,
    sum(cb_amount) over (order by days_bucket) / sum(cb_amount) over () as cumulative_share_amount,
    days_bucket > {{ var('maturity_window_days') }} as is_beyond_agreed_window
from buckets
order by 1
