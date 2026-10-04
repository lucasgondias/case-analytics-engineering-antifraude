-- Grão: dia x regra (só regras que decidem). O falso alerta de fraude típico é UMA regra
-- disparando fora do padrão; a taxa de recusa total pode nem se mexer. Z-score por regra
-- contra os 28 dias anteriores.
with rule_days as (
    select
        attempts.cohort_date as metric_date,
        hits.rule_id
    from {{ ref('stg_risk__rule_hits') }} as hits
    inner join {{ ref('stg_risk__evaluations') }} as ev
        on hits.evaluation_id = ev.evaluation_id
    inner join {{ ref('fct_payment_attempts') }} as attempts
        on ev.transaction_id = attempts.transaction_id
    where not hits.is_shadow
),

volume as (
    select
        cohort_date as metric_date,
        count(*) as evaluated_count
    from {{ ref('fct_payment_attempts') }}
    where evaluation_id is not null
    group by 1
),

daily as (
    select
        rule_days.metric_date,
        rule_days.rule_id,
        count(*)::double / max(volume.evaluated_count) as hit_rate
    from rule_days
    inner join volume on rule_days.metric_date = volume.metric_date
    group by 1, 2
),

with_baseline as (
    select
        *,
        avg(hit_rate) over w as baseline_hit_rate,
        stddev_samp(hit_rate) over w as stddev_hit_rate
    from daily
    window w as (partition by rule_id order by metric_date rows between 28 preceding and 1 preceding)
)

select
    metric_date,
    rule_id,
    hit_rate,
    baseline_hit_rate,
    (hit_rate - baseline_hit_rate) / nullif(stddev_hit_rate, 0) as zscore_hit_rate,
    case
        when abs((hit_rate - baseline_hit_rate) / nullif(stddev_hit_rate, 0)) > 3
            then 'P2_regra_fora_do_padrao'
        when baseline_hit_rate is null then 'sem_base_historica'
        else 'ok'
    end as alert_status
from with_baseline
