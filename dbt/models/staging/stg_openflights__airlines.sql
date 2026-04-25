{{
    config(
        materialized='view',
        tags=['staging', 'openflights'],
    )
}}

{%- set src = source('bronze', 'openflights_airlines_raw') -%}

select
    try_to_number(openflights_id)::int     as openflights_id,
    nullif(trim(name), '')                 as carrier_name,
    nullif(trim(alias), '\\N')             as alias,
    nullif(upper(trim(iata)), '\\N')       as carrier_key,
    nullif(upper(trim(icao)), '\\N')       as icao_code,
    nullif(trim(callsign), '\\N')          as callsign,
    nullif(trim(country), '')              as country,
    case upper(trim(active))
        when 'Y' then true
        when 'N' then false
        else null
    end                                     as is_active,
    current_timestamp()                    as ingested_at
from {{ src }}
where iata is not null
  and length(trim(iata)) = 2
