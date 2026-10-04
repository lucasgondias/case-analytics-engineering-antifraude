{#- Data de referência para idade de safra: var as_of_date (testes/reprodução) ou current_date. -#}
{% macro as_of_date() -%}
    {%- if var('as_of_date', none) is not none -%}
        date '{{ var("as_of_date") }}'
    {%- else -%}
        current_date
    {%- endif -%}
{%- endmacro %}
