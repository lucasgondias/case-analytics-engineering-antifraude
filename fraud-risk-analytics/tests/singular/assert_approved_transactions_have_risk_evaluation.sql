-- ERRO SILENCIOSO #1: transação aprovada sem avaliação de risco.
-- Causa provável: motor em fail-open (timeout -> aprova) ou perda de evento no feed de risco.
-- Impacto: aprovação "sem rede" e performance do motor superestimada.
-- Pega no case: tx_1005.
{{ config(severity='error', warn_if='>0', error_if='>10') }}

select
    transaction_id,
    cohort_date,
    amount,
    payment_method
from {{ ref('fct_payment_attempts') }}
where is_approved_without_risk_evaluation
