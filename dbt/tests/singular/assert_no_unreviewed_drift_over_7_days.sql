{# Per PIPELINE.md and INCIDENTS.md #1: any unreviewed schema-drift entry
   older than 7 days fails CI. #}

select
    detected_at,
    source_name,
    table_name,
    kind,
    column_name
from {{ var('drift_log_database') }}.{{ var('drift_log_schema') }}.{{ var('drift_log_table') }}
where kind in ('added', 'removed')
  and reviewed_at is null
  and detected_at < dateadd(day, -7, current_timestamp())
