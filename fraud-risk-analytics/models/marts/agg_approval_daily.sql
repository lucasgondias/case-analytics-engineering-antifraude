-- Grão: dia x método de pagamento. Decompõe a recusa por ORIGEM para isolar o impacto
-- das regras de risco na conversão (status sozinho não diz quem recusou).
with attempts as (
    select * from {{ ref('fct_payment_attempts') }}
),

orders as (
    -- Aprovação por intenção de compra: pedido aprovado se QUALQUER tentativa foi aprovada.
    -- Retentativas inflam "tentadas" na métrica oficial; esta é a visão do cliente.
    select
        cohort_date,
        payment_method,
        order_id,
        bool_or(is_approved) as is_order_approved
    from attempts
    where order_id is not null
    group by 1, 2, 3
),

order_totals as (
    select
        cohort_date,
        payment_method,
        count(*) as order_count,
        count(*) filter (where is_order_approved) as approved_order_count
    from orders
    group by 1, 2
),

attempt_totals as (
    select
        cohort_date,
        payment_method,
        count(*) as attempt_count,
        count(*) filter (where is_approved) as approved_count,
        count(*) filter (where decline_source = 'risk_engine') as declined_by_risk_count,
        count(*) filter (where decline_source = 'manual_review') as declined_by_manual_review_count,
        count(*) filter (where decline_source = 'issuer_or_acquirer') as declined_by_issuer_count,
        count(*) filter (where issuer_decline_type = 'soft') as issuer_soft_decline_count,
        count(*) filter (where issuer_decline_type = 'hard') as issuer_hard_decline_count,
        count(*) filter (where decline_source = 'technical_error') as technical_error_count,
        count(*) filter (where decline_source = 'unknown_no_risk_evaluation') as declined_unknown_count,
        count(*) filter (where decision_source = 'manual_review') as manual_review_count,
        count(*) filter (where is_duplicate_candidate) as duplicate_candidate_count,
        count(*) filter (where is_approved_without_risk_evaluation)
            as approved_without_risk_evaluation_count,
        coalesce(sum(amount), 0) as attempted_amount,
        coalesce(sum(amount) filter (where is_approved), 0) as approved_amount
    from attempts
    group by 1, 2
)

select
    attempt_totals.cohort_date as transaction_date,
    attempt_totals.* exclude (cohort_date),
    coalesce(order_totals.order_count, 0) as order_count,
    coalesce(order_totals.approved_order_count, 0) as approved_order_count
from attempt_totals
left join order_totals
    on
        attempt_totals.cohort_date = order_totals.cohort_date
        and attempt_totals.payment_method = order_totals.payment_method
