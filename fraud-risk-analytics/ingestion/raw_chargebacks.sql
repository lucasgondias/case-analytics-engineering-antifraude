-- Lakeflow Declarative Pipelines: Bronze de chargebacks (arquivos das adquirentes via Auto Loader).
-- Contrato: contracts/acquirer_chargebacks.odcs.yaml. O Auto Loader registra os arquivos já lidos:
-- reprocessar não duplica. A chegada de arquivo também dispara o job de chargeback (resources/).
-- As condições abaixo são o complemento exato das da quarentena: manter as duas listas iguais.

CREATE OR REFRESH STREAMING TABLE raw_chargebacks (
    CONSTRAINT chargeback_id_presente EXPECT (chargeback_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT transaction_id_presente EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT chargeback_at_valido EXPECT (try_to_timestamp(chargeback_at) IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT cb_amount_positivo EXPECT (coalesce(try_cast(cb_amount AS DECIMAL(18, 2)) > 0, false)) ON VIOLATION DROP ROW
)
COMMENT 'Bronze: chargebacks recebidos das adquirentes, só acréscimo.'
AS SELECT
    chargeback_id,
    transaction_id,
    chargeback_at,
    reason_code,
    cb_amount,
    current_timestamp() AS _ingested_at,
    _metadata.file_path AS _source_file
FROM STREAM read_files(
    '${landing_path}/chargebacks/',
    format => 'csv',
    header => true,
    schema => 'chargeback_id STRING, transaction_id STRING, chargeback_at STRING, reason_code STRING, cb_amount STRING'
);

CREATE OR REFRESH STREAMING TABLE quarantine_chargebacks
COMMENT 'Chargebacks que violam o contrato. Cada linha gera aviso ao responsável pela adquirente.'
AS SELECT
    chargeback_id,
    transaction_id,
    chargeback_at,
    reason_code,
    cb_amount,
    current_timestamp() AS _ingested_at,
    _metadata.file_path AS _source_file
FROM STREAM read_files(
    '${landing_path}/chargebacks/',
    format => 'csv',
    header => true,
    schema => 'chargeback_id STRING, transaction_id STRING, chargeback_at STRING, reason_code STRING, cb_amount STRING'
)
WHERE NOT (
    chargeback_id IS NOT NULL
    AND transaction_id IS NOT NULL
    AND try_to_timestamp(chargeback_at) IS NOT NULL
    AND coalesce(try_cast(cb_amount AS DECIMAL(18, 2)) > 0, false)
);
