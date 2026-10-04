{#-
    Funções que diferem entre bancos ficam isoladas aqui. O resto do SQL usa só sintaxe comum a
    DuckDB (protótipo) e Databricks SQL (produção), ou os macros cross-database do próprio dbt
    (dbt.datediff, dbt.dateadd, dbt.listagg).
-#}

{% macro regex_match(column, pattern) -%}
    {%- if target.type == 'duckdb' -%}
        regexp_matches({{ column }}, '{{ pattern }}')
    {%- else -%}
        {{ column }} rlike '{{ pattern }}'
    {%- endif -%}
{%- endmacro %}

{% macro iso_day_of_week(column) -%}
    {#- 1 = segunda ... 7 = domingo -#}
    {%- if target.type == 'duckdb' -%}
        isodow({{ column }})
    {%- else -%}
        ((dayofweek({{ column }}) + 5) % 7) + 1
    {%- endif -%}
{%- endmacro %}

{% macro daily_date_series(start_date, end_date) -%}
    {%- if target.type == 'duckdb' -%}
        select cast(range as date) as date_day
        from range(date '{{ start_date }}', date '{{ end_date }}', interval 1 day)
    {%- else -%}
        select explode(sequence(date '{{ start_date }}', date '{{ end_date }}', interval 1 day)) as date_day
    {%- endif -%}
{%- endmacro %}
