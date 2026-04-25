{{ config(materialized='ephemeral', tags=['intermediate', 'reconciliation']) }}

{# Reconcile BTS flight events with OpenSky landing detections.
   See INTERVIEW_PREP.md Q15 for the rationale.

   Joining is deliberately lossy: dim_aircraft maps tail_number → icao24, but
   tail numbers get reused over time. We pick the best landing within
   ±2 hours of scheduled_arr_ts and fall back to scheduled if none. #}

with bts as (
    select * from {{ ref('int_flight_event_bts') }}
),

aircraft as (
    select tail_number, icao24
    from {{ ref('stg_bts__flights') }}
    where tail_number is not null and tail_number <> ''
    qualify row_number() over (
        partition by tail_number
        order by flight_date desc
    ) = 1
),

landings as (
    select * from {{ ref('int_landing_events') }}
),

bts_with_icao as (
    select b.*, a.icao24
    from bts b
    left join aircraft a on b.tail_number = a.tail_number
),

candidate_landings as (
    select
        b.flight_event_natural_key,
        l.live_landing_ts,
        abs(timestampdiff(second, l.live_landing_ts, b.scheduled_arr_ts)) as ts_diff_s
    from bts_with_icao b
    inner join landings l
        on b.icao24 is not null
        and b.icao24 = l.icao24
        and l.live_landing_ts between
            timestampadd(hour, -2, b.scheduled_arr_ts)
            and timestampadd(hour, 2, b.scheduled_arr_ts)
),

best_landing as (
    select
        flight_event_natural_key,
        live_landing_ts as live_actual_arr_ts
    from candidate_landings
    qualify row_number() over (
        partition by flight_event_natural_key
        order by ts_diff_s asc
    ) = 1
)

select
    b.flight_event_natural_key,
    b.flight_date,
    b.carrier_code,
    b.flight_number,
    b.tail_number,
    b.origin_iata,
    b.dest_iata,
    b.scheduled_dep_ts,
    b.actual_dep_ts,
    b.scheduled_arr_ts,
    b.actual_arr_ts,
    b.dep_delay_min,
    b.arr_delay_min,
    b.cancelled_flag,
    b.cancellation_code,
    b.diverted_flag,
    b.distance_miles,
    b.bronze_loaded_at,
    bl.live_actual_arr_ts,
    case
        when b.actual_arr_ts is not null and bl.live_actual_arr_ts is not null
             and abs(timestampdiff(minute, b.actual_arr_ts, bl.live_actual_arr_ts)) <= 15 then 'matched'
        when b.actual_arr_ts is not null and bl.live_actual_arr_ts is not null then 'mismatch'
        when b.actual_arr_ts is not null then 'bts_only'
        when bl.live_actual_arr_ts is not null then 'live_only'
        else 'bts_only'
    end as reconciliation_status,
    case when bl.live_actual_arr_ts is not null and b.actual_arr_ts is null
         then 'opensky_reconciled'
         else 'bts'
    end as source_system
from bts_with_icao b
left join best_landing bl using (flight_event_natural_key)
