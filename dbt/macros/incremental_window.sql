{# =============================================================================
   incremental_window(date_col, late_arrival_days)

   Standard predicate for incremental models. On a regular run, only consider
   rows whose date column lies within the late-arrival window. On a backfill
   (--vars '{backfill_start: ..., backfill_end: ...}'), use the explicit
   bounds and skip the late-arrival window.
   ============================================================================= #}

{% macro incremental_window(date_col, late_arrival_days) %}
    {%- set bf_start = var('backfill_start', '') -%}
    {%- set bf_end   = var('backfill_end',   '') -%}

    {%- if bf_start and bf_end -%}
        {{ date_col }} between to_date('{{ bf_start }}') and to_date('{{ bf_end }}')
    {%- elif is_incremental() -%}
        {{ date_col }} >= dateadd(day, -{{ late_arrival_days }},
            (select coalesce(max({{ date_col }}), to_date('1900-01-01')) from {{ this }}))
    {%- else -%}
        true
    {%- endif -%}
{% endmacro %}
