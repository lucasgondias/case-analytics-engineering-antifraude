-- Grão: 1 linha por transaction_id com a decisão de risco VIGENTE no momento da autorização
-- (última avaliação até transaction_at + tolerância de relógio).
-- Avaliações posteriores à autorização são contadas mas não definem a decisão.
with evaluations as (
    select * from {{ ref('stg_risk__evaluations') }}
),

transactions as (
    select
        transaction_id,
        transaction_at
    from {{ ref('stg_payments__transactions') }}
),

joined as (
    select
        evaluations.*,
        transactions.transaction_at,
        evaluations.evaluated_at
        <= {{ dbt.dateadd("second", var('clock_skew_tolerance_seconds'), "transactions.transaction_at") }}
            as is_pre_authorization
    from evaluations
    inner join transactions on evaluations.transaction_id = transactions.transaction_id
)

select
    transaction_id,
    evaluation_id,
    evaluated_at,
    risk_score,
    risk_action,
    rule_triggered,
    decision_source,
    count(*) over (partition by transaction_id) as evaluation_count,
    count(*) filter (where not is_pre_authorization) over (partition by transaction_id)
        as post_authorization_evaluation_count
from joined
qualify row_number() over (
    partition by transaction_id
    order by is_pre_authorization desc, evaluated_at desc, evaluation_id desc
) = 1
