-- Dimensão de data conformada: safra (cohort_date), mês de notificação e calendário do painel.
select
    date_day,
    cast(date_trunc('month', date_day) as date) as month_start,
    extract(year from date_day) as year_number,
    extract(month from date_day) as month_number,
    {{ iso_day_of_week('date_day') }} as iso_day_of_week,
    {{ iso_day_of_week('date_day') }} in (6, 7) as is_weekend
from {{ ref('metricflow_time_spine') }}
