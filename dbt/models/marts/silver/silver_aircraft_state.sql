{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['icao24', 'event_ts'],
        table_format='iceberg',
        external_volume=var('iceberg_external_volume'),
        catalog=var('iceberg_catalog'),
        base_location_root=var('iceberg_base_location'),
        cluster_by=['icao24', 'event_ts'],
        tags=['silver', 'aircraft_state'],
    )
}}

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
    bronze_loaded_at,
    current_timestamp() as ingested_at
from {{ ref('int_aircraft_state_clean') }}
where {{ incremental_window('cast(event_ts as date)', var('opensky_late_arrival_days')) }}
