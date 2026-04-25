{# =============================================================================
   log_unknown_columns(source_name, table_name, expected_columns)

   Compares the live column list of a Glue-backed source table against the
   expected list, then writes any drift (added or removed columns) to
   ${drift_log_database}.${drift_log_schema}.${drift_log_table}.

   Run via:
       dbt run-operation log_unknown_columns \
           --args '{source_name: bronze, table_name: bts_raw,
                    expected_columns: [flightdate, reporting_airline, ...]}'

   The Slack alert + CI gate from PIPELINE.md / INCIDENTS.md #1 reads from
   this table. Always emits at least one row per run (kind=run_marker) so
   downstream freshness tests can detect a stalled drift checker.
   ============================================================================= #}

{% macro log_unknown_columns(source_name, table_name, expected_columns=[]) %}
    {%- set rel = source(source_name, table_name) -%}
    {%- set live_cols = adapter.get_columns_in_relation(rel) | map(attribute='name') | map('lower') | list -%}
    {%- set expected_lower = expected_columns | map('lower') | list -%}

    {%- set added = [] -%}
    {%- for c in live_cols if c not in expected_lower -%}
        {%- do added.append(c) -%}
    {%- endfor -%}

    {%- set removed = [] -%}
    {%- for c in expected_lower if c not in live_cols -%}
        {%- do removed.append(c) -%}
    {%- endfor -%}

    {%- set log_db = var('drift_log_database') -%}
    {%- set log_schema = var('drift_log_schema') -%}
    {%- set log_table = var('drift_log_table') -%}
    {%- set fqn = log_db ~ '.' ~ log_schema ~ '.' ~ log_table -%}

    {%- set ddl %}
        create schema if not exists {{ log_db }}.{{ log_schema }};
        create table if not exists {{ fqn }} (
            detected_at  timestamp_ntz default current_timestamp(),
            source_name  string,
            table_name   string,
            kind         string,           -- added | removed | run_marker
            column_name  string,
            reviewed_at  timestamp_ntz,
            reviewed_by  string,
            notes        string
        );
    {%- endset %}
    {% do run_query(ddl) %}

    {%- set rows = [] -%}
    {%- do rows.append("(current_timestamp(), '" ~ source_name ~ "', '" ~ table_name ~ "', 'run_marker', null, null, null, null)") -%}
    {%- for c in added -%}
        {%- do rows.append("(current_timestamp(), '" ~ source_name ~ "', '" ~ table_name ~ "', 'added', '" ~ c ~ "', null, null, null)") -%}
    {%- endfor -%}
    {%- for c in removed -%}
        {%- do rows.append("(current_timestamp(), '" ~ source_name ~ "', '" ~ table_name ~ "', 'removed', '" ~ c ~ "', null, null, null)") -%}
    {%- endfor -%}

    {%- set insert_sql -%}
        insert into {{ fqn }} (detected_at, source_name, table_name, kind, column_name, reviewed_at, reviewed_by, notes)
        values
        {{ rows | join(',\n        ') }}
    {%- endset -%}
    {% do run_query(insert_sql) %}

    {{ log("[log_unknown_columns] " ~ source_name ~ "." ~ table_name
            ~ " added=" ~ added | length ~ " removed=" ~ removed | length, info=True) }}
{% endmacro %}
