{# =============================================================================
   drop_ci_schema(schema)

   Drops a per-PR CI schema in FLIGHTPULSE_DEV. Called from .github/workflows/
   ci.yml at the end of a successful run so we don't accumulate dead schemas.
   Idempotent — `drop schema if exists` swallows missing schemas.

   Use with care: this is *only* safe to call against FLIGHTPULSE_DEV.<schema>
   where <schema> begins with 'ci_pr_'. The guard below enforces that.
   ============================================================================= #}

{% macro drop_ci_schema(schema) %}
    {%- if not schema or not schema.startswith('ci_pr_') -%}
        {{ exceptions.raise_compiler_error(
            "drop_ci_schema refuses to drop a schema not prefixed 'ci_pr_': " ~ schema
        ) }}
    {%- endif -%}
    {%- set sql -%}
        drop schema if exists FLIGHTPULSE_DEV.{{ schema }} cascade;
    {%- endset -%}
    {% if execute %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
