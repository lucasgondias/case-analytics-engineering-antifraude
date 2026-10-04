-- Grão: regra x versão x modo (enforced/shadow).
-- Shadow mode: a regra avalia mas não decide. Cruzando o que ela TERIA feito com o rótulo de
-- fraude, temos backtest antes de ligar a regra, e precisão/cobertura de cada regra ativa.
with hits as (
    select * from {{ ref('stg_risk__rule_hits') }}
),

evaluations as (
    select
        evaluation_id,
        transaction_id
    from {{ ref('stg_risk__evaluations') }}
),

attempts as (
    select
        transaction_id,
        amount,
        is_approved,
        is_fraud_labeled
    from {{ ref('fct_payment_attempts') }}
),

labeled_universe as (
    select count(*) filter (where is_fraud_labeled) as total_fraud_transactions
    from attempts
),

rule_hits as (
    select
        hits.rule_id,
        hits.rule_version,
        hits.is_shadow,
        hits.rule_decision,
        attempts.transaction_id,
        attempts.amount,
        attempts.is_approved,
        attempts.is_fraud_labeled,
        count(*) over (partition by attempts.transaction_id) as rules_hit_on_transaction
    from hits
    inner join evaluations on hits.evaluation_id = evaluations.evaluation_id
    inner join attempts on evaluations.transaction_id = attempts.transaction_id
)

select
    rule_hits.rule_id,
    rule_hits.rule_version,
    case when rule_hits.is_shadow then 'shadow' else 'enforced' end as rule_mode,
    count(*) as hit_count,
    count(*) filter (where rule_hits.rule_decision = 'reject') as would_reject_count,
    count(*) filter (where rule_hits.is_fraud_labeled) as hits_on_fraud,
    -- precisão: das transações que a regra pegou, quantas eram fraude (só mede o que tem rótulo)
    count(*) filter (where rule_hits.is_fraud_labeled)::double / nullif(count(*), 0)
        as precision_on_labeled,
    -- cobertura: da fraude total rotulada, quanto a regra pegaria
    count(*) filter (where rule_hits.is_fraud_labeled)::double
    / nullif(max(labeled_universe.total_fraud_transactions), 0) as fraud_coverage,
    -- custo em conversão: aprovadas sem rótulo de fraude que a regra recusaria
    count(*) filter (
        where
        rule_hits.rule_decision = 'reject'
        and rule_hits.is_approved
        and not rule_hits.is_fraud_labeled
    ) as good_approved_would_block_count,
    coalesce(sum(rule_hits.amount) filter (
        where rule_hits.rule_decision = 'reject' and rule_hits.is_fraud_labeled
    ), 0) as fraud_amount_would_block,
    -- sobreposição: % dos hits em transações que outra regra também pegou
    count(*) filter (where rule_hits.rules_hit_on_transaction > 1)::double / nullif(count(*), 0)
        as overlap_share
from rule_hits
cross join labeled_universe
group by 1, 2, 3
