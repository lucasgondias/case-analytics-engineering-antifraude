-- Grão: 1 linha por transação com ao menos uma evidência de fraude.
-- O rótulo é o PRIMEIRO sinal, venha de onde vier: TC40/SAFE chega dias antes do chargeback.
with labels as (
    select * from {{ ref('fct_fraud_labels') }}
),

first_label as (
    select
        transaction_id,
        min(label_at) as first_fraud_label_at,
        min_by(label_source, label_at) as first_fraud_label_source,
        count(*) as fraud_evidence_count
    from labels
    group by transaction_id
),

sources as (
    select distinct
        transaction_id,
        label_source
    from labels
),

source_list as (
    select
        transaction_id,
        {{ dbt.listagg('label_source', "','", 'order by label_source') }} as fraud_label_sources
    from sources
    group by transaction_id
)

select
    first_label.transaction_id,
    first_label.first_fraud_label_at,
    first_label.first_fraud_label_source,
    first_label.fraud_evidence_count,
    source_list.fraud_label_sources
from first_label
inner join source_list on first_label.transaction_id = source_list.transaction_id
