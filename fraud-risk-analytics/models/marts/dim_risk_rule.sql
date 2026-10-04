-- Dimensão de regras do motor: cada versão é uma linha (regra muda de versão, não de chave).
-- is_shadow distingue regra que só avalia de regra que decide.
select distinct
    rule_id || '@v' || cast(rule_version as varchar) as rule_key,
    rule_id,
    rule_version,
    is_shadow
from {{ ref('stg_risk__rule_hits') }}
