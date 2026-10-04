{#-
    Filtro incremental com SOBREPOSIÇÃO: _ingested_at > watermark - lookback.
    Watermark puro (> max) perde evento que chega fora de ordem (carregadores paralelos,
    reentrega). O MERGE por chave torna o reprocessamento da sobreposição idempotente.
-#}
{% macro incremental_since(watermark_column) -%}
    (select max({{ watermark_column }}) from {{ this }})
        - to_hours({{ var('incremental_lookback_hours') }})
{%- endmacro %}
