-- Motor rejeitou e o pagamento foi aprovado: override sem trilha ou bug de orquestração.
select
    transaction_id,
    risk_action,
    status
from {{ ref('fct_payment_attempts') }}
where is_approved_despite_risk_reject
