{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='flight_event_sk',
        cluster_by=['flight_date_key', 'carrier_key'],
        tags=['gold', 'fact', 'critical'],
    )
}}

{# Native Snowflake fact, clustered on (flight_date, carrier_code) per
   ARCHITECTURE.md. Joins fact silver to current dims. #}

with src as (
    select * from {{ ref('silver_flight_event') }}
    where {{ incremental_window('flight_date', var('bts_late_arrival_days')) }}
),

current_airport as (
    select airport_key, airport_sk
    from {{ ref('dim_airport') }}
    where is_current
),

carrier as (
    select carrier_key from {{ ref('dim_carrier') }}
)

select
    md5(src.flight_event_natural_key)             as flight_event_sk,
    src.flight_date                                as flight_date_key,
    src.carrier_code                               as carrier_key,
    src.origin_iata                                as origin_airport_key,
    src.dest_iata                                  as dest_airport_key,
    orig.airport_sk                                as origin_airport_sk,
    dest.airport_sk                                as dest_airport_sk,
    src.tail_number                                as aircraft_key,
    src.flight_number,
    src.scheduled_dep_ts,
    src.actual_dep_ts,
    src.scheduled_arr_ts,
    src.actual_arr_ts,
    src.dep_delay_min,
    src.arr_delay_min,
    src.cancelled_flag,
    src.cancellation_code,
    src.diverted_flag,
    src.distance_miles,
    src.live_actual_arr_ts,
    src.reconciliation_status,
    src.source_system,
    current_timestamp()                            as ingested_at,
    sha2(
        src.flight_event_natural_key
        || '|' || coalesce(to_varchar(src.actual_dep_ts), '')
        || '|' || coalesce(to_varchar(src.actual_arr_ts), '')
        || '|' || coalesce(to_varchar(src.cancelled_flag), '')
        || '|' || coalesce(src.cancellation_code, '')
    , 256)                                         as record_hash
from src
left join current_airport orig on src.origin_iata = orig.airport_key
left join current_airport dest on src.dest_iata   = dest.airport_key
left join carrier             on src.carrier_code = carrier.carrier_key
