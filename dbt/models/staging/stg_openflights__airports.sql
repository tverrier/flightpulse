{{
    config(
        materialized='view',
        tags=['staging', 'openflights'],
    )
}}

{%- set src = source('bronze', 'openflights_airports_raw') -%}

select
    try_to_number(openflights_id)::int      as openflights_id,
    nullif(trim(name), '')                  as name,
    nullif(trim(city), '')                  as city,
    nullif(trim(country), '')               as country,
    nullif(upper(trim(iata)), '\\N')        as iata,
    nullif(upper(trim(icao)), '\\N')        as icao,
    try_to_number(latitude)::float          as latitude,
    try_to_number(longitude)::float         as longitude,
    try_to_number(altitude)::int            as elevation_ft,
    try_to_number(timezone_offset)::float   as utc_offset_hours,
    nullif(trim(dst), '\\N')                as dst,
    nullif(trim(tz_database), '\\N')        as timezone,
    nullif(trim(type), '\\N')               as airport_type,
    nullif(trim(source), '\\N')             as source_system,
    current_timestamp()                     as ingested_at
from {{ src }}
where iata is not null
  and length(trim(iata)) = 3
