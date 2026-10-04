-- Grão: 1 linha por transação com ao menos uma evidência de fraude.
-- O rótulo é o PRIMEIRO sinal, venha de onde vier: TC40/SAFE chega dias antes do chargeback.
with labels as (
    select * from {{ ref('fct_fraud_labels') }}
)

select
    transaction_id,
    min(label_at) as first_fraud_label_at,
    arg_min(label_source, label_at) as first_fraud_label_source,
    count(*) as fraud_evidence_count,
    string_agg(distinct label_source, ',' order by label_source) as fraud_label_sources
from labels
group by transaction_id
