-- Grão: dia x regra x origem da decisão x faixa de score.
-- Performance do motor = chargeback de fraude sobre o que ele APROVOU (falso negativo).
-- Limitação estrutural: transação rejeitada não tem label (viés de seleção). Medir falso
-- positivo exige grupo de controle (holdout aleatório aprovado). Ver docs/decisoes.md.
select
    cohort_date as transaction_date,
    coalesce(rule_triggered, 'no_rule') as rule_triggered,
    coalesce(decision_source, 'no_evaluation') as decision_source,
    case
        when risk_score is null then 'sem_score'
        when risk_score < 30 then '00-29'
        when risk_score < 60 then '30-59'
        when risk_score < 80 then '60-79'
        else '80-100'
    end as score_band,
    count(*) as transaction_count,
    count(*) filter (where risk_action = 'approve') as risk_approved_count,
    count(*) filter (where risk_action = 'reject') as risk_rejected_count,
    count(*) filter (where risk_action = 'approve' and has_fraud_chargeback) as false_negative_count,
    coalesce(sum(amount) filter (where risk_action = 'approve' and has_fraud_chargeback), 0)
        as false_negative_amount,
    coalesce(sum(amount) filter (where risk_action = 'reject'), 0) as rejected_amount,
    avg(risk_score) as avg_risk_score
from {{ ref('fct_payment_attempts') }}
group by 1, 2, 3, 4
