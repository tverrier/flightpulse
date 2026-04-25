{{
    config(
        materialized='table',
        tags=['gold', 'dim', 'critical'],
    )
}}

{# SCD-2 view over the dim_airport snapshot. The snapshot tracks
   valid_from / valid_to / dbt_valid_to; we expose them as the SCD-2 surface
   for downstream joins. #}

with snap as (
    select * from {{ ref('snp_dim_airport') }}
)

select
    md5(iata || '|' || coalesce(to_varchar(dbt_valid_from), '')) as airport_sk,
    iata                                       as airport_key,
    icao                                       as icao_code,
    name,
    city,
    country,
    null::string                               as state,         -- BTS adds this; keep null until joined
    latitude,
    longitude,
    elevation_ft,
    timezone,
    cast(dbt_valid_from as date)               as valid_from,
    cast(dbt_valid_to   as date)               as valid_to,
    dbt_valid_to is null                       as is_current,
    current_timestamp()                        as ingested_at
from snap
where iata is not null
