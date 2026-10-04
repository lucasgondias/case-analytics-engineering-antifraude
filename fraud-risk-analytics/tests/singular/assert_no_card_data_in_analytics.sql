-- PCI DSS: número de cartão (PAN), CVV e validade nunca chegam à camada analítica.
-- Só token, fingerprint, BIN e últimos 4 dígitos. Qualquer coluna com esses nomes bloqueia.
select
    table_schema,
    table_name,
    column_name
from information_schema.columns
where
    table_schema in ('raw', 'staging', 'intermediate', 'marts', 'monitoring')
    and regexp_matches(
        lower(column_name),
        '^(pan|card_number|cardnumber|cvv|cvc|card_cvv|expiry|expiration_date|card_expiry)$'
    )
