{{ config(materialized='table') }}
-- Time spine diário exigido pela camada semântica (MetricFlow).
{{ daily_date_series('2025-01-01', '2027-12-31') }}
