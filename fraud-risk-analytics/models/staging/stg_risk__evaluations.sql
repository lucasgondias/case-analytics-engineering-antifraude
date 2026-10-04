-- Grão: 1 linha por evaluation_id. Uma transação pode ter N avaliações (re-score, step-up 3DS).
with source as (
    select * from {{ source('raw', 'raw_risk_evaluations') }}
),

typed as (
    select
        trim(evaluation_id) as evaluation_id,
        trim(transaction_id) as transaction_id,
        try_cast(evaluated_at as timestamp) as evaluated_at,
        -- Score chega como string ('05'). Cast explícito + teste de range 0-100.
        try_cast(risk_score as integer) as risk_score,
        lower(trim(risk_action)) as risk_action,
        nullif(lower(trim(rule_triggered)), '') as rule_triggered,
        _ingested_at
    from source
)

select
    *,
    -- rule_triggered mistura "regra disparada" com "desfecho de revisão manual".
    -- Separamos para não creditar ao motor automático uma decisão humana.
    case
        when rule_triggered like 'rule_manual_review%' then 'manual_review'
        else 'automatic'
    end as decision_source
from typed
qualify row_number() over (partition by evaluation_id order by _ingested_at desc) = 1
