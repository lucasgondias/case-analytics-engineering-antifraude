{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['cohort_date', 'payment_method']
    )
}}
-- Grão: safra (data da transação) x método de pagamento. Só NUMERADORES e DENOMINADORES
-- aditivos: taxa é calculada na camada semântica (razão de somas, nunca média de taxas).
--
-- Incremental: recalcula apenas as safras tocadas por linhas atualizadas no fato.
-- Só guarda o que é função do dado (nunca da data de execução), senão o incremental congela valor.
-- Chargeback de 2026-03-15 para tx de 2026-03-01 reabre a safra 2026-03-01.

with attempts as (
    select * from {{ ref('fct_payment_attempts') }}
    {% if is_incremental() %}
        where cohort_date in (
            select distinct cohort_date
            from {{ ref('fct_payment_attempts') }}
            where _updated_at > (select max(_updated_at) from {{ this }})
        )
    {% endif %}
)

select
    cohort_date,
    payment_method,
    bool_or(is_chargeback_eligible) as is_chargeback_eligible,

    count(*) as attempt_count,
    count(*) filter (where is_approved) as approved_count,
    coalesce(sum(amount) filter (where is_approved), 0) as approved_amount,

    count(*) filter (where is_approved and has_chargeback) as chargeback_transaction_count,
    count(*) filter (where is_approved and has_fraud_chargeback)
        as fraud_chargeback_transaction_count,
    coalesce(sum(chargeback_amount) filter (where is_approved), 0) as chargeback_amount,
    coalesce(sum(fraud_chargeback_amount) filter (where is_approved), 0)
        as fraud_chargeback_amount,

    -- versões sem candidatos a duplicidade (análise de sensibilidade)
    count(*) filter (where is_approved and not is_duplicate_candidate) as approved_count_dedup,
    coalesce(sum(amount) filter (where is_approved and not is_duplicate_candidate), 0)
        as approved_amount_dedup,

    -- rótulo de fraude (qualquer evidência: TC40/SAFE, chargeback, reembolso, MED)
    count(*) filter (where is_approved and is_fraud_labeled) as fraud_labeled_count,
    coalesce(sum(amount) filter (where is_approved and is_fraud_labeled), 0) as fraud_labeled_amount,
    coalesce(sum(fraud_refund_amount) filter (where is_approved), 0) as fraud_refund_amount,

    -- B2: idade e maturidade NÃO são gravadas aqui (dependem de "hoje" e ficariam congeladas
    -- nas safras não reprocessadas). Calculadas na leitura em rpt_chargeback_cohort_maturity.
    max(_updated_at) as _updated_at
from attempts
group by cohort_date, payment_method
