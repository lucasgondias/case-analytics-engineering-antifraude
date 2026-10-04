-- PSI (Population Stability Index) diário do risk_score contra a base dos 28 dias anteriores.
-- Drift do score = modelo vendo população diferente da de treino: causa clássica de falso
-- alerta em massa. Limiares: > 0,10 atenção; > 0,25 incidente (docs/decisoes.md).
with scored as (
    select
        cohort_date as metric_date,
        least(floor(risk_score / 10), 9) as score_bucket
    from {{ ref('fct_payment_attempts') }}
    where risk_score is not null
),

daily as (
    select
        metric_date,
        score_bucket,
        count(*) as n
    from scored
    group by 1, 2
),

days as (
    select distinct metric_date from daily
),

buckets as (
    select range as score_bucket from range(0, 10)
),

grid as (
    select
        days.metric_date,
        buckets.score_bucket
    from days
    cross join buckets
),

actual as (
    select
        grid.metric_date,
        grid.score_bucket,
        coalesce(daily.n, 0) as n,
        sum(coalesce(daily.n, 0)) over (partition by grid.metric_date) as day_total
    from grid
    left join daily
        on grid.metric_date = daily.metric_date and grid.score_bucket = daily.score_bucket
),

expected as (
    select
        days.metric_date,
        daily.score_bucket,
        sum(daily.n) as n
    from days
    inner join daily
        on
            daily.metric_date >= days.metric_date - interval 28 day
            and days.metric_date > daily.metric_date
    group by 1, 2
),

expected_share as (
    select
        metric_date,
        score_bucket,
        n::double / sum(n) over (partition by metric_date) as share
    from expected
),

compared as (
    select
        actual.metric_date,
        greatest(actual.n::double / nullif(actual.day_total, 0), 0.0001) as actual_share,
        greatest(coalesce(expected_share.share, 0), 0.0001) as expected_share,
        expected_share.share is not null as has_expected
    from actual
    left join expected_share
        on
            actual.metric_date = expected_share.metric_date
            and actual.score_bucket = expected_share.score_bucket
),

psi as (
    select
        metric_date,
        sum((actual_share - expected_share) * ln(actual_share / expected_share)) as psi,
        bool_or(has_expected) as has_baseline
    from compared
    group by 1
)

select
    metric_date,
    case when has_baseline then psi end as psi,
    case
        when not has_baseline then 'sem_base_historica'
        when psi > 0.25 then 'P1_drift_severo'
        when psi > 0.10 then 'P2_drift_moderado'
        else 'ok'
    end as alert_status
from psi
