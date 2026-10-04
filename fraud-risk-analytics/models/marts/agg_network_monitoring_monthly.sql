-- Grão: lojista x bandeira x mês calendário.
-- É a SEGUNDA taxa de chargeback, diferente da taxa por safra:
--   safra   -> "o motor errou nas vendas de março?" (atribuído à data da venda)
--   bandeira -> "vamos tomar multa ou ser descredenciados este mês?" (atribuído à data da notificação)
-- Regras (referência 2026, parametrizadas em dbt_project.yml > network_programs):
--   Visa VAMP  = (fraude reportada TC40 + disputas TC15) / transações liquidadas do mês (cartão não presente)
--   MC ECM     = chargebacks do mês / transações do MÊS ANTERIOR, com mínimo de chargebacks
--   MC EFM     = valor de chargeback de fraude / valor transacionado, em bps
{%- set p = var('network_programs') %}

with attempts as (
    select * from {{ ref('fct_payment_attempts') }}
    where is_chargeback_eligible and merchant_id is not null
),

settled as (
    select
        merchant_id,
        card_network,
        cast(date_trunc('month', settled_at) as date) as month_start,
        count(*) as settled_count,
        sum(amount) as settled_amount
    from attempts
    where is_settled
    group by 1, 2, 3
),

fraud_reports as (
    select
        attempts.merchant_id,
        reports.card_network,
        cast(date_trunc('month', reports.reported_at) as date) as month_start,
        count(*) as fraud_report_count
    from {{ ref('stg_network__issuer_fraud_reports') }} as reports
    inner join attempts on reports.transaction_id = attempts.transaction_id
    group by 1, 2, 3
),

disputes as (
    select
        merchant_id,
        card_network,
        cast(date_trunc('month', chargeback_at) as date) as month_start,
        count(*) as dispute_count,
        count(*) filter (where is_fraud_reason) as fraud_dispute_count,
        sum(cb_amount) filter (where is_fraud_reason) as fraud_dispute_amount
    from {{ ref('fct_chargebacks') }}
    where merchant_id is not null
    group by 1, 2, 3
),

spine as (
    select
        merchant_id,
        card_network,
        month_start
    from settled
    union
    select
        merchant_id,
        card_network,
        month_start
    from fraud_reports
    union
    select
        merchant_id,
        card_network,
        month_start
    from disputes
),

monthly as (
    select
        spine.merchant_id,
        spine.card_network,
        spine.month_start,
        coalesce(settled.settled_count, 0) as settled_count,
        coalesce(settled.settled_amount, 0) as settled_amount,
        coalesce(fraud_reports.fraud_report_count, 0) as fraud_report_count,
        coalesce(disputes.dispute_count, 0) as dispute_count,
        coalesce(disputes.fraud_dispute_count, 0) as fraud_dispute_count,
        coalesce(disputes.fraud_dispute_amount, 0) as fraud_dispute_amount,
        -- mês calendário anterior por join (lag pularia meses sem movimento)
        coalesce(previous_settled.settled_count, 0) as previous_month_settled_count
    from spine
    left join settled
        on
            spine.merchant_id = settled.merchant_id
            and spine.card_network = settled.card_network
            and spine.month_start = settled.month_start
    left join settled as previous_settled
        on
            spine.merchant_id = previous_settled.merchant_id
            and spine.card_network = previous_settled.card_network
            and cast(spine.month_start - interval 1 month as date) = previous_settled.month_start
    left join fraud_reports
        on
            spine.merchant_id = fraud_reports.merchant_id
            and spine.card_network = fraud_reports.card_network
            and spine.month_start = fraud_reports.month_start
    left join disputes
        on
            spine.merchant_id = disputes.merchant_id
            and spine.card_network = disputes.card_network
            and spine.month_start = disputes.month_start
),

ratios as (
    select
        *,
        -- VAMP: fraude reportada e disputa contam juntas (fraude com chargeback conta duas vezes)
        case
            when card_network = 'visa'
                then cast((fraud_report_count + dispute_count) as double) / nullif(settled_count, 0)
        end as visa_vamp_ratio,
        case
            when card_network = 'mastercard'
                then cast(dispute_count as double) / nullif(previous_month_settled_count, 0)
        end as mc_ecm_ratio,
        case
            when card_network = 'mastercard'
                then 10000.0 * fraud_dispute_amount / nullif(settled_amount, 0)
        end as mc_efm_fraud_bps,
        -- projeção intramês: no mês corrente, extrapola contagens pelo ritmo até hoje
        case
            when month_start = date_trunc('month', {{ as_of_date() }})
                then
                    cast(datediff('day', month_start, cast(month_start + interval 1 month as date)) as double)
                    / greatest(datediff('day', month_start, {{ as_of_date() }}) + 1, 1)
            else 1.0
        end as projection_factor
    from monthly
)

select
    *,
    round(dispute_count * projection_factor) as projected_dispute_count,
    round(settled_count * projection_factor) as projected_settled_count,
    case
        when card_network = 'visa' and settled_count * projection_factor < {{ p.visa_vamp_min_settled_txn }}
            then 'abaixo_do_piso_de_volume'
        when visa_vamp_ratio >= {{ p.visa_vamp_merchant_ratio }} then 'acima_do_limite'
        when
            visa_vamp_ratio >= {{ p.visa_vamp_merchant_ratio * p.early_warning_share }}
            then 'alerta_preventivo'
        when card_network = 'visa' then 'ok'
    end as visa_vamp_status,
    case
        when card_network = 'mastercard' and previous_month_settled_count = 0 then 'sem_base_mes_anterior'
        when
            mc_ecm_ratio >= {{ p.mc_ecm_ratio }}
            and dispute_count * projection_factor >= {{ p.mc_ecm_min_chargebacks }}
            then 'acima_do_limite'
        when mc_ecm_ratio >= {{ p.mc_ecm_ratio * p.early_warning_share }} then 'alerta_preventivo'
        when card_network = 'mastercard' then 'ok'
    end as mc_ecm_status,
    case
        when mc_efm_fraud_bps >= {{ p.mc_efm_fraud_bps }} then 'acima_do_limite'
        when card_network = 'mastercard' then 'ok'
    end as mc_efm_status
from ratios
