{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='flight_event_natural_key',
        table_format='iceberg',
        external_volume=var('iceberg_external_volume'),
        catalog=var('iceberg_catalog'),
        base_location_root=var('iceberg_base_location'),
        cluster_by=['carrier_code', 'flight_date'],
        tags=['silver', 'flight_event'],
    )
}}

{# Silver fact source-of-truth, Iceberg-materialized to S3 via the external
   volume. Snowflake writes the manifests; dbt owns the merge. #}

select
    flight_event_natural_key,
    flight_date,
    carrier_code,
    flight_number,
    tail_number,
    origin_iata,
    dest_iata,
    scheduled_dep_ts,
    actual_dep_ts,
    scheduled_arr_ts,
    actual_arr_ts,
    dep_delay_min,
    arr_delay_min,
    cancelled_flag,
    cancellation_code,
    diverted_flag,
    distance_miles,
    live_actual_arr_ts,
    reconciliation_status,
    source_system,
    bronze_loaded_at,
    current_timestamp() as ingested_at
from {{ ref('int_flight_event_reconciled') }}
where {{ incremental_window('flight_date', var('bts_late_arrival_days')) }}
