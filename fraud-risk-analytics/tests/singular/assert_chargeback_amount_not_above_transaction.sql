-- cb_amount > amount: moeda diferente, tarifa embutida ou chargebacks somados em duplicidade.
{{ config(severity='warn') }}

select
    transaction_id,
    sum(cb_amount) as total_cb_amount,
    max(transaction_amount) as transaction_amount
from {{ ref('fct_chargebacks') }}
where not is_orphan
group by transaction_id
having sum(cb_amount) > max(transaction_amount)
