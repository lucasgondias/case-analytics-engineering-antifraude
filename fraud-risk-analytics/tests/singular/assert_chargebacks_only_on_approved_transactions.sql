-- Chargeback em transação recusada não deveria existir: indica status errado na origem
-- (ex.: aprovada pela adquirente mas registrada como declined) ou id trocado.
{{ config(severity='warn') }}

select
    chargeback_id,
    transaction_id,
    transaction_status
from {{ ref('fct_chargebacks') }}
where not is_orphan and is_on_non_approved_transaction
