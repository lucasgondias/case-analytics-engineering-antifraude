-- Grão: 1 linha por transaction_id que compartilha chave natural com outra transação
-- (mesmo usuário, valor, método, dentro de 60s). Pode ser:
--   a) reentrega do gateway com novo id  -> problema de dado (dupla contagem)
--   b) cobrança em dobro real (double click) -> problema de produto/financeiro
-- Não removemos: sinalizamos e o consumidor decide. A métrica oficial segue o contrato.
with transactions as (
    select * from {{ ref('stg_payments__transactions') }}
),

pairs as (
    select
        a.transaction_id,
        min(b.transaction_id) as first_transaction_id_in_group,
        count(*) as group_size
    from transactions as a
    inner join transactions as b
        on
            a.user_id = b.user_id
            and a.amount = b.amount
            and a.payment_method = b.payment_method
            and abs(datediff('second', a.transaction_at, b.transaction_at)) <= 60
    group by a.transaction_id
    having count(*) > 1
)

select
    transaction_id,
    first_transaction_id_in_group,
    group_size,
    transaction_id <> first_transaction_id_in_group as is_duplicate_candidate
from pairs
