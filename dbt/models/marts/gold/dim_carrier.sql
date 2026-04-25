{{
    config(
        materialized='table',
        tags=['gold', 'dim', 'critical'],
    )
}}

{# SCD-1: overwrite. dim_carrier is wholly derived from OpenFlights;
   no history kept. #}

with src as (
    select * from {{ ref('stg_openflights__airlines') }}
)

select
    carrier_key,
    carrier_name,
    icao_code,
    country,
    coalesce(is_active, true) as is_active,
    cast('1987-01-01' as date) as valid_from,
    null::date                  as valid_to,
    current_timestamp()         as ingested_at
from src
where carrier_key is not null
qualify row_number() over (
    partition by carrier_key
    order by case when icao_code is not null then 0 else 1 end, openflights_id
) = 1
