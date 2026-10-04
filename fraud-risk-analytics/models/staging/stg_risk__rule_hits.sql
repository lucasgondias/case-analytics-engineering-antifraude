-- Grão: avaliação x regra. Regra em shadow avalia mas não decide: base do backtest.
select
    trim(evaluation_id) as evaluation_id,
    trim(rule_id) as rule_id,
    try_cast(rule_version as integer) as rule_version,
    lower(trim(is_shadow)) = 'true' as is_shadow,
    lower(trim(rule_decision)) as rule_decision,
    _ingested_at
from {{ source('raw_v2', 'raw_risk_rule_hits') }}
qualify row_number() over (
    partition by evaluation_id, rule_id, rule_version order by _ingested_at desc
) = 1
