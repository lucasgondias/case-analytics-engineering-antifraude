-- Chargeback sem transação: perda de dado na ingestão de transações ou transação de outra
-- entidade/ambiente. Fica no fct_chargebacks (Perda Bruta), mas some do CB rate por safra.
{{ config(severity='warn') }}

select
    chargeback_id,
    transaction_id,
    cb_amount
from {{ ref('fct_chargebacks') }}
where is_orphan
