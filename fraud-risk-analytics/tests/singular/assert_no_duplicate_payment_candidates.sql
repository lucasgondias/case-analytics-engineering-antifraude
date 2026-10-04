-- ERRO SILENCIOSO #2: mesmo usuário, valor e método em <= 60s com transaction_id diferente.
-- Reentrega do gateway (infla aprovação e denominador do CB rate) ou cobrança em dobro real.
-- Pega no case: tx_1005 (duplicata de tx_1004).
{{ config(severity='warn') }}

select
    transaction_id,
    duplicate_of_transaction_id,
    user_id,
    amount,
    transaction_at
from {{ ref('fct_payment_attempts') }}
where is_duplicate_candidate
