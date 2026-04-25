{# =============================================================================
   get_column_or_null(column_name, alias=None, source_relation=None)

   Returns the column reference if it exists in the source relation;
   otherwise emits `NULL AS <alias>`. Lets staging models survive a
   removed/renamed BTS column without failing the dbt run — see
   INCIDENTS.md § Incident 1.

   Usage:
       select
           {{ get_column_or_null('Div_Airport_Landings', alias='div_airport_landings',
                                 source_relation=source('bronze','bts_raw')) }}
       from {{ source('bronze','bts_raw') }}
   ============================================================================= #}

{% macro get_column_or_null(column_name, alias=None, source_relation=None) %}
    {%- set out_alias = alias if alias else column_name | lower -%}

    {%- if execute and source_relation is not none -%}
        {%- set cols = adapter.get_columns_in_relation(source_relation) -%}
        {%- set names = cols | map(attribute='name') | map('lower') | list -%}
        {%- if column_name | lower in names -%}
            {{ adapter.quote(column_name) }} as {{ out_alias }}
        {%- else -%}
            cast(null as varchar) as {{ out_alias }}
        {%- endif -%}
    {%- else -%}
        {# parse-time fallback: optimistic — emit the column reference. #}
        {{ column_name }} as {{ out_alias }}
    {%- endif -%}
{% endmacro %}
