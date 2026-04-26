{{
    config(
        materialized='table',
        tags=['gold', 'dim'],
    )
}}

{# SCD-1, distinct tail numbers seen in BTS, joined to icao24 from OpenSky.
   Manufacturer/model/year/seat_capacity are sourced from the FAA registry in
   a real deployment; we leave them NULL here and document the upgrade path
   in DATA_MODEL.md. #}

with bts_tails as (
    select
        tail_number                            as aircraft_key,
        max(carrier_code)                      as operator_carrier_key
    from {{ ref('stg_bts__flights') }}
    where tail_number is not null
    group by 1
)

select
    bt.aircraft_key,
    null::string             as icao24,             -- backfilled by Phase 5 sensor
    null::string             as manufacturer,
    null::string             as model,
    bt.operator_carrier_key,
    null::int                as year_built,
    null::int                as seat_capacity,
    current_timestamp()      as ingested_at
from bts_tails bt
