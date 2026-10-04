-- Grão: dia x método de pagamento. Decompõe a recusa por ORIGEM para isolar o impacto
-- das regras de risco na conversão (status sozinho não diz quem recusou).
select
    cohort_date as transaction_date,
    payment_method,
    count(*) as attempt_count,
    count(*) filter (where is_approved) as approved_count,
    count(*) filter (where decline_source = 'risk_engine') as declined_by_risk_count,
    count(*) filter (where decline_source = 'issuer_or_acquirer') as declined_by_issuer_count,
    count(*) filter (where decline_source = 'technical_error') as technical_error_count,
    count(*) filter (where decline_source = 'unknown_no_risk_evaluation') as declined_unknown_count,
    count(*) filter (where is_duplicate_candidate) as duplicate_candidate_count,
    count(*) filter (where is_approved_without_risk_evaluation)
        as approved_without_risk_evaluation_count,
    coalesce(sum(amount), 0) as attempted_amount,
    coalesce(sum(amount) filter (where is_approved), 0) as approved_amount
from {{ ref('fct_payment_attempts') }}
group by 1, 2
