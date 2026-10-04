-- Grão: 1 linha por dia. Sinais de ERRO SILENCIOSO que nenhum teste de schema pega:
-- o dado está "válido", mas o comportamento mudou. Alimenta alerta (Slack/PagerDuty) e o
-- painel de saúde do time de Risco. Limiares em docs/decisoes.md.
with daily as (
    select
        cohort_date as metric_date,
        count(*) as attempt_count,
        count(*) filter (where is_approved) as approved_count,
        count(*) filter (where decline_source = 'risk_engine') as risk_rejected_count,
        count(*) filter (where is_approved_without_risk_evaluation) as approved_without_evaluation_count,
        count(*) filter (where is_duplicate_candidate) as duplicate_candidate_count,
        count(*) filter (where decline_source = 'technical_error') as technical_error_count,
        avg(risk_score) as avg_risk_score
    from {{ ref('fct_payment_attempts') }}
    group by 1
),

with_rates as (
    select
        *,
        approved_count::double / nullif(attempt_count, 0) as approval_rate,
        risk_rejected_count::double / nullif(attempt_count, 0) as risk_reject_rate,
        approved_without_evaluation_count::double / nullif(approved_count, 0)
            as approved_without_evaluation_rate
    from daily
),

with_baseline as (
    -- Baseline móvel de 28 dias (exclui o próprio dia) para z-score de volume e taxas.
    select
        *,
        avg(attempt_count) over w as baseline_attempts,
        stddev_samp(attempt_count) over w as stddev_attempts,
        avg(approval_rate) over w as baseline_approval_rate,
        stddev_samp(approval_rate) over w as stddev_approval_rate,
        avg(risk_reject_rate) over w as baseline_risk_reject_rate,
        stddev_samp(risk_reject_rate) over w as stddev_risk_reject_rate
    from with_rates
    window w as (order by metric_date rows between 28 preceding and 1 preceding)
)

select
    metric_date,
    attempt_count,
    approval_rate,
    risk_reject_rate,
    approved_without_evaluation_rate,
    duplicate_candidate_count,
    technical_error_count,
    avg_risk_score,
    (attempt_count - baseline_attempts) / nullif(stddev_attempts, 0) as zscore_attempts,
    (approval_rate - baseline_approval_rate) / nullif(stddev_approval_rate, 0) as zscore_approval_rate,
    (risk_reject_rate - baseline_risk_reject_rate) / nullif(stddev_risk_reject_rate, 0)
        as zscore_risk_reject_rate,
    -- severidade consolidada para roteamento de alerta
    case
        when approved_without_evaluation_rate > 0.01 then 'P1_fail_open_motor'
        when abs((risk_reject_rate - baseline_risk_reject_rate) / nullif(stddev_risk_reject_rate, 0)) > 3
            then 'P2_regra_disparando_fora_do_padrao'
        when abs((attempt_count - baseline_attempts) / nullif(stddev_attempts, 0)) > 3
            then 'P2_volume_anomalo'
        when duplicate_candidate_count > 0 then 'P3_duplicidade'
        else 'ok'
    end as alert_status
from with_baseline
