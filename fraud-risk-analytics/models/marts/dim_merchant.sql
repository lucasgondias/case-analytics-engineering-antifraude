-- Dimensão conformada de lojista (star schema). Atributos descritivos lentos; os fatos guardam
-- só a chave. Em produção vira snapshot SCD2 (segmento e status do lojista mudam no tempo).
select
    merchant_id,
    segment,
    mcc,
    merchant_created_at
from {{ ref('stg_merchants') }}
