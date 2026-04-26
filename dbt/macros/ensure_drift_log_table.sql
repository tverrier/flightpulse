{# =============================================================================
   ensure_drift_log_table()

   Idempotent DDL for the schema-drift log. Called from on-run-start so the
   singular test assert_no_unreviewed_drift_over_7_days.sql can run on a clean
   warehouse before the first `dbt run-operation log_unknown_columns`.
   ============================================================================= #}

{% macro ensure_drift_log_table() %}
    {%- set log_db     = var('drift_log_database') -%}
    {%- set log_schema = var('drift_log_schema') -%}
    {%- set log_table  = var('drift_log_table') -%}
    {%- set fqn = log_db ~ '.' ~ log_schema ~ '.' ~ log_table -%}

    {%- set ddl -%}
        create schema if not exists {{ log_db }}.{{ log_schema }};
        create table if not exists {{ fqn }} (
            detected_at  timestamp_ntz default current_timestamp(),
            source_name  string,
            table_name   string,
            kind         string,
            column_name  string,
            reviewed_at  timestamp_ntz,
            reviewed_by  string,
            notes        string
        );
    {%- endset -%}

    {% if execute %}
        {% do run_query(ddl) %}
    {% endif %}
{% endmacro %}
