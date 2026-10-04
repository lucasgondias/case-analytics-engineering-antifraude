-- A decisão vigente precisa ser anterior à autorização (com tolerância de relógio).
-- Avaliação posterior = desalinhamento de relógio ou re-score que não decidiu nada.
select
    t.transaction_id,
    t.transaction_at,
    r.evaluated_at
from {{ ref('stg_payments__transactions') }} as t
inner join {{ ref('int_risk__decision_per_transaction') }} as r
    on t.transaction_id = r.transaction_id
where r.evaluated_at > t.transaction_at + to_seconds({{ var('clock_skew_tolerance_seconds') }})
