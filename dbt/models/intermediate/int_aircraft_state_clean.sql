{{ config(materialized='ephemeral', tags=['intermediate', 'opensky']) }}

{# Per-(icao24, event_ts) latest record. OpenSky may publish duplicates if
   the consumer restarts within a minute; pick the most recent bronze file. #}

with src as (
    select * from {{ ref('stg_opensky__states') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by icao24, event_ts
            order by bronze_loaded_at desc
        ) as rn
    from src
)

select
    icao24,
    callsign,
    origin_country,
    event_ts,
    longitude,
    latitude,
    baro_altitude_m,
    geo_altitude_m,
    on_ground,
    velocity_mps,
    true_track_deg,
    vertical_rate_mps,
    squawk,
    position_source,
    event_date,
    event_hour,
    bronze_loaded_at
from ranked
where rn = 1
