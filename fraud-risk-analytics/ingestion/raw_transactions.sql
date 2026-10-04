-- Lakeflow Declarative Pipelines: Bronze de tentativas de pagamento (eventos do Kafka).
-- Contrato: contracts/payments_transactions.odcs.yaml. Campos mantidos como texto (cópia fiel);
-- a tipagem acontece no staging do dbt. Registro que viola o contrato vai para a quarentena.
-- As condições abaixo são o complemento exato das da quarentena: manter as duas listas iguais.

CREATE OR REFRESH STREAMING TABLE raw_transactions (
    CONSTRAINT transaction_id_presente EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT transaction_at_valido EXPECT (try_to_timestamp(transaction_at) IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT amount_positivo EXPECT (coalesce(try_cast(amount AS DECIMAL(18, 2)) > 0, false)) ON VIOLATION DROP ROW,
    CONSTRAINT status_aceito EXPECT (coalesce(status IN ('approved', 'declined', 'error'), false)) ON VIOLATION DROP ROW,
    CONSTRAINT payment_method_presente EXPECT (payment_method IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Bronze: tentativas de pagamento, só acréscimo.'
AS SELECT
    payload.transaction_id,
    payload.user_id,
    payload.transaction_at,
    payload.amount,
    payload.status,
    payload.payment_method,
    current_timestamp() AS _ingested_at
FROM (
    SELECT from_json(
        CAST(value AS STRING),
        'transaction_id STRING, user_id STRING, transaction_at STRING, amount STRING, status STRING, payment_method STRING'
    ) AS payload
    FROM STREAM read_kafka(
        bootstrapServers => '${kafka_bootstrap_servers}',
        subscribe => 'payments.transactions',
        startingOffsets => 'earliest'
    )
);

CREATE OR REFRESH STREAMING TABLE quarantine_transactions
COMMENT 'Tentativas que violam o contrato. Cada linha gera aviso ao time de Pagamentos.'
AS SELECT
    CAST(value AS STRING) AS raw_payload,
    payload.*,
    current_timestamp() AS _ingested_at
FROM (
    SELECT
        value,
        from_json(
            CAST(value AS STRING),
            'transaction_id STRING, user_id STRING, transaction_at STRING, amount STRING, status STRING, payment_method STRING'
        ) AS payload
    FROM STREAM read_kafka(
        bootstrapServers => '${kafka_bootstrap_servers}',
        subscribe => 'payments.transactions',
        startingOffsets => 'earliest'
    )
)
WHERE NOT (
    payload.transaction_id IS NOT NULL
    AND try_to_timestamp(payload.transaction_at) IS NOT NULL
    AND coalesce(try_cast(payload.amount AS DECIMAL(18, 2)) > 0, false)
    AND coalesce(payload.status IN ('approved', 'declined', 'error'), false)
    AND payload.payment_method IS NOT NULL
);
