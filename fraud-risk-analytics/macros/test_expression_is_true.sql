{#- Equivalente ao dbt_utils.expression_is_true, sem dependência de pacote. -#}
{% test expression_is_true(model, column_name, expression) %}
select *
from {{ model }}
where not ({{ column_name }} {{ expression }})
{% endtest %}
