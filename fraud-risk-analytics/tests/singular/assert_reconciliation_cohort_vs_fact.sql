-- O agregado por safra precisa bater com o fato (detecta safra não reprocessada no incremental).
with fact as (
    select
        cohort_date,
        payment_method,
        count(*) filter (where is_approved) as approved_count,
        coalesce(sum(chargeback_amount) filter (where is_approved), 0) as chargeback_amount
    from {{ ref('fct_payment_attempts') }}
    group by 1, 2
)

select
    fact.*,
    agg.approved_count as agg_approved_count,
    agg.chargeback_amount as agg_chargeback_amount
from fact
full outer join {{ ref('agg_chargeback_cohort_daily') }} as agg
    on fact.cohort_date = agg.cohort_date and fact.payment_method = agg.payment_method
where
    fact.approved_count is distinct from agg.approved_count
    or fact.chargeback_amount is distinct from agg.chargeback_amount
