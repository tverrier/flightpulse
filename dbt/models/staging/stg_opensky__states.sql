{{
    config(
        materialized='view',
        tags=['staging', 'opensky'],
    )
}}

{# Cleansed view over the OpenSky Parquet rolls. Casts to typed columns and
   drops obviously-broken rows. Heavy dedupe is left to int_aircraft_state_clean. #}

{%- set src = source('bronze', 'opensky_raw') -%}

with raw as (
    select
        lower(trim(icao24))           as icao24,
        nullif(trim(callsign), '')    as callsign,
        origin_country,
        try_to_number(event_ts)::bigint        as event_ts_epoch,
        try_to_number(time_position)::bigint   as time_position_epoch,
        try_to_number(last_contact)::bigint    as last_contact_epoch,
        try_to_number(longitude)::float        as longitude,
        try_to_number(latitude)::float         as latitude,
        try_to_number(baro_altitude)::float    as baro_altitude_m,
        coalesce(try_to_boolean(on_ground), false) as on_ground,
        try_to_number(velocity)::float         as velocity_mps,
        try_to_number(true_track)::float       as true_track_deg,
        try_to_number(vertical_rate)::float    as vertical_rate_mps,
        try_to_number(geo_altitude)::float     as geo_altitude_m,
        nullif(trim(squawk), '')               as squawk,
        try_to_number(position_source)::int    as position_source,
        ds                                      as ds,
        try_to_number(hr)                      as hr,
        _metadata$file_last_modified           as bronze_loaded_at
    from {{ src }}
    where icao24 is not null
)

select
    icao24,
    callsign,
    origin_country,
    to_timestamp_ntz(event_ts_epoch)            as event_ts,
    to_timestamp_ntz(time_position_epoch)       as time_position_ts,
    to_timestamp_ntz(last_contact_epoch)        as last_contact_ts,
    longitude,
    latitude,
    baro_altitude_m,
    on_ground,
    velocity_mps,
    true_track_deg,
    vertical_rate_mps,
    geo_altitude_m,
    squawk,
    position_source,
    ds                                          as event_date,
    hr                                          as event_hour,
    bronze_loaded_at
from raw
where event_ts_epoch is not null
  and event_ts_epoch between 946684800 and 4102444800   -- 2000-01-01..2100-01-01
