{{ config(materialized='ephemeral', tags=['intermediate', 'bts']) }}

{# BTS-derived flight events. Builds the timestamp_tz scheduled / actual
   timestamps from the airport tz on origin/dest, and converts BTS HHMM ints
   to time-of-day. The origin tz join is the canonical lookup for departure
   times; arrival times use destination tz. #}

with f as (
    select * from {{ ref('stg_bts__flights') }}
),

orig_tz as (
    select iata, timezone from {{ ref('stg_openflights__airports') }}
),

dest_tz as (
    select iata, timezone from {{ ref('stg_openflights__airports') }}
),

joined as (
    select
        f.*,
        orig_tz.timezone as origin_timezone,
        dest_tz.timezone as dest_timezone
    from f
    left join orig_tz on f.origin_iata = orig_tz.iata
    left join dest_tz on f.dest_iata = dest_tz.iata
),

with_ts as (
    select
        flight_date,
        carrier_code,
        flight_number,
        tail_number,
        origin_iata,
        dest_iata,
        origin_timezone,
        dest_timezone,
        distance_miles,
        cancelled_flag,
        cancellation_code,
        diverted_flag,
        dep_delay_min,
        arr_delay_min,
        bronze_loaded_at,
        flight_event_natural_key,

        -- Scheduled timestamps in the local tz of the relevant airport,
        -- promoted to TIMESTAMP_TZ.
        case when crs_dep_time_hhmm is not null
             then convert_timezone(
                    coalesce(origin_timezone, 'UTC'),
                    'UTC',
                    timestampadd(
                        minute,
                        mod(crs_dep_time_hhmm, 100) + 60 * floor(crs_dep_time_hhmm / 100),
                        to_timestamp_ntz(flight_date)
                    )
                  )
        end as scheduled_dep_ts,

        case when crs_arr_time_hhmm is not null
             then convert_timezone(
                    coalesce(dest_timezone, 'UTC'),
                    'UTC',
                    timestampadd(
                        minute,
                        mod(crs_arr_time_hhmm, 100) + 60 * floor(crs_arr_time_hhmm / 100),
                        to_timestamp_ntz(flight_date)
                    )
                  )
        end as scheduled_arr_ts,

        case when dep_time_hhmm is not null and not cancelled_flag
             then convert_timezone(
                    coalesce(origin_timezone, 'UTC'),
                    'UTC',
                    timestampadd(
                        minute,
                        mod(dep_time_hhmm, 100) + 60 * floor(dep_time_hhmm / 100),
                        to_timestamp_ntz(flight_date)
                    )
                  )
        end as actual_dep_ts,

        case when arr_time_hhmm is not null and not cancelled_flag
             then convert_timezone(
                    coalesce(dest_timezone, 'UTC'),
                    'UTC',
                    timestampadd(
                        minute,
                        mod(arr_time_hhmm, 100) + 60 * floor(arr_time_hhmm / 100),
                        to_timestamp_ntz(flight_date)
                    )
                  )
        end as actual_arr_ts
    from joined
)

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
    bronze_loaded_at
from with_ts
