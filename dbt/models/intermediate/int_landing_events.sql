{{ config(materialized='ephemeral', tags=['intermediate', 'opensky']) }}

{# Detect landings from OpenSky state vectors:
   1. previous state was airborne (on_ground=false) AND velocity > 30 m/s
   2. current state is on_ground=true
   3. transition timestamp = current event_ts

   Rare false positives from telemetry gaps are acceptable — the
   reconciliation step prefers BTS actual when present. #}

with s as (
    select * from {{ ref('int_aircraft_state_clean') }}
),

with_prev as (
    select
        s.*,
        lag(on_ground) over (partition by icao24 order by event_ts)        as prev_on_ground,
        lag(velocity_mps) over (partition by icao24 order by event_ts)     as prev_velocity_mps,
        lag(latitude) over (partition by icao24 order by event_ts)         as prev_latitude,
        lag(longitude) over (partition by icao24 order by event_ts)        as prev_longitude
    from s
)

select
    icao24,
    callsign,
    event_ts                        as live_landing_ts,
    latitude,
    longitude,
    prev_velocity_mps               as approach_velocity_mps,
    bronze_loaded_at
from with_prev
where on_ground = true
  and prev_on_ground = false
  and coalesce(prev_velocity_mps, 0) > 30
