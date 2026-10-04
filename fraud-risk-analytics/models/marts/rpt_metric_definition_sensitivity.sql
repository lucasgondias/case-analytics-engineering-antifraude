-- Mesmo dado, definições diferentes -> números diferentes.
-- Evidência de por que a métrica precisa de UMA definição versionada (camada semântica).
with attempts as (
    select * from {{ ref('fct_payment_attempts') }}
),

scenarios as (
    select s.*
    from (
        values
        (1, 'Contrato literal (todos os métodos, com duplicidade)', true, true),
        (2, 'Sem candidatos a duplicidade', true, false),
        (3, 'Só métodos elegíveis a chargeback (cartão)', false, true),
        (4, 'Cartão e sem duplicidade', false, false)
    ) as s (scenario_order, scenario, include_non_card, include_duplicates)
),

filtered as (
    select
        scenarios.scenario_order,
        scenarios.scenario,
        attempts.*
    from scenarios
    inner join attempts
        on
            (scenarios.include_non_card or attempts.is_chargeback_eligible)
            and (scenarios.include_duplicates or not attempts.is_duplicate_candidate)
)

select
    scenario_order,
    scenario,
    count(*) as attempt_count,
    count(*) filter (where is_approved) as approved_count,
    round(100.0 * count(*) filter (where is_approved) / count(*), 2) as approval_rate_pct,
    count(*) filter (where is_approved and has_chargeback) as chargeback_transaction_count,
    round(
        100.0 * count(*) filter (where is_approved and has_chargeback)
        / nullif(count(*) filter (where is_approved), 0), 2
    ) as chargeback_rate_qty_pct,
    sum(amount) filter (where is_approved) as approved_amount,
    sum(chargeback_amount) filter (where is_approved) as chargeback_amount,
    round(
        100.0 * sum(chargeback_amount) filter (where is_approved)
        / nullif(sum(amount) filter (where is_approved), 0), 2
    ) as chargeback_rate_amount_pct
from filtered
group by 1, 2
order by 1
