{{ config(materialized='ephemeral', tags=['intermediate', 'reconciliation']) }}

{# Reconcile BTS flight events with OpenSky landing detections.
   See INTERVIEW_PREP.md Q15 for the rationale.

   BTS does not publish ICAO24, so we derive a (carrier, flight_number) ->
   icao24 lookup from OpenSky callsigns (typically `AAL123`, `DAL0456`).
   This is best-effort: callsigns can be reused across days and codeshares
   blur the mapping, so we pick the most-recent icao24 per (carrier,
   flight_number) and accept that some rows will land as `bts_only`.
   Replace with the FAA registry join when available. #}

with bts as (
    select * from {{ ref('int_flight_event_bts') }}
),

opensky_callsigns as (
    select
        icao24,
        callsign,
        regexp_substr(callsign, '^[A-Z]{2,3}')               as cs_carrier,
        try_to_number(regexp_substr(callsign, '[0-9]+$'))    as cs_flight_num,
        event_ts
    from {{ ref('stg_opensky__states') }}
    where callsign is not null
      and regexp_like(callsign, '^[A-Z]{2,3}[0-9]+$')
),

aircraft as (
    select cs_carrier as carrier_code,
           to_varchar(cs_flight_num) as flight_number,
           icao24
    from opensky_callsigns
    where cs_carrier is not null
      and cs_flight_num is not null
    qualify row_number() over (
        partition by cs_carrier, cs_flight_num
        order by event_ts desc
    ) = 1
),

landings as (
    select * from {{ ref('int_landing_events') }}
),

bts_with_icao as (
    select b.*, a.icao24
    from bts b
    left join aircraft a
        on b.carrier_code = a.carrier_code
        and b.flight_number = a.flight_number
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
