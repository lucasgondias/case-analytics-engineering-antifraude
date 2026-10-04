-- Lakeflow Declarative Pipelines: Bronze de avaliações do motor de risco (eventos do Kafka).
-- Contrato: contracts/risk_engine_evaluations.odcs.yaml. risk_score segue como texto ('05');
-- a conversão para inteiro e o teste de faixa ficam no staging do dbt.
-- As condições abaixo são o complemento exato das da quarentena: manter as duas listas iguais.

CREATE OR REFRESH STREAMING TABLE raw_risk_evaluations (
    CONSTRAINT evaluation_id_presente EXPECT (evaluation_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT transaction_id_presente EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT evaluated_at_valido EXPECT (try_to_timestamp(evaluated_at) IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT risk_action_aceita EXPECT (coalesce(risk_action IN ('approve', 'reject', 'review'), false)) ON VIOLATION DROP ROW
)
COMMENT 'Bronze: avaliações do motor de risco, só acréscimo.'
AS SELECT
    payload.evaluation_id,
    payload.transaction_id,
    payload.evaluated_at,
    payload.risk_score,
    payload.risk_action,
    payload.rule_triggered,
    current_timestamp() AS _ingested_at
FROM (
    SELECT from_json(
        CAST(value AS STRING),
        'evaluation_id STRING, transaction_id STRING, evaluated_at STRING, risk_score STRING, risk_action STRING, rule_triggered STRING'
    ) AS payload
    FROM STREAM read_kafka(
        bootstrapServers => '${kafka_bootstrap_servers}',
        subscribe => 'risk.evaluations',
        startingOffsets => 'earliest'
    )
);

CREATE OR REFRESH STREAMING TABLE quarantine_risk_evaluations
COMMENT 'Avaliações que violam o contrato. Cada linha gera aviso ao time do motor de risco.'
AS SELECT
    CAST(value AS STRING) AS raw_payload,
    payload.*,
    current_timestamp() AS _ingested_at
FROM (
    SELECT
        value,
        from_json(
            CAST(value AS STRING),
            'evaluation_id STRING, transaction_id STRING, evaluated_at STRING, risk_score STRING, risk_action STRING, rule_triggered STRING'
        ) AS payload
    FROM STREAM read_kafka(
        bootstrapServers => '${kafka_bootstrap_servers}',
        subscribe => 'risk.evaluations',
        startingOffsets => 'earliest'
    )
)
WHERE NOT (
    payload.evaluation_id IS NOT NULL
    AND payload.transaction_id IS NOT NULL
    AND try_to_timestamp(payload.evaluated_at) IS NOT NULL
    AND coalesce(payload.risk_action IN ('approve', 'reject', 'review'), false)
);
