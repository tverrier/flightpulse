{{
    config(
        materialized='view',
        tags=['staging', 'bts'],
    )
}}

{# Staging select for the BTS bronze CSV. Uses get_column_or_null so a
   removed/renamed source column degrades to NULL instead of breaking the run
   (Incident #1 prevention). New columns are surfaced via
   `dbt run-operation log_unknown_columns` — see PIPELINE.md § Schema Drift. #}

{%- set src = source('bronze', 'bts_raw') -%}

with raw as (
    select
        {{ get_column_or_null('FlightDate',                          alias='flight_date_raw',          source_relation=src) }},
        {{ get_column_or_null('Reporting_Airline',                   alias='carrier_code',             source_relation=src) }},
        {{ get_column_or_null('Flight_Number_Reporting_Airline',     alias='flight_number',            source_relation=src) }},
        {{ get_column_or_null('Tail_Number',                         alias='tail_number',              source_relation=src) }},
        {{ get_column_or_null('Origin',                              alias='origin_iata',              source_relation=src) }},
        {{ get_column_or_null('Dest',                                alias='dest_iata',                source_relation=src) }},
        {{ get_column_or_null('CRSDepTime',                          alias='crs_dep_time',             source_relation=src) }},
        {{ get_column_or_null('DepTime',                             alias='dep_time',                 source_relation=src) }},
        {{ get_column_or_null('DepDelay',                            alias='dep_delay_min_raw',        source_relation=src) }},
        {{ get_column_or_null('CRSArrTime',                          alias='crs_arr_time',             source_relation=src) }},
        {{ get_column_or_null('ArrTime',                             alias='arr_time',                 source_relation=src) }},
        {{ get_column_or_null('ArrDelay',                            alias='arr_delay_min_raw',        source_relation=src) }},
        {{ get_column_or_null('Cancelled',                           alias='cancelled_raw',            source_relation=src) }},
        {{ get_column_or_null('CancellationCode',                    alias='cancellation_code',        source_relation=src) }},
        {{ get_column_or_null('Diverted',                            alias='diverted_raw',             source_relation=src) }},
        {{ get_column_or_null('Distance',                            alias='distance_miles_raw',       source_relation=src) }},
        {# Coalesce old/new BTS naming; keep both alive for backward compat. #}
        coalesce(
            {{ get_column_or_null('Div_Airport_Landings',  source_relation=src) }},
            {{ get_column_or_null('DivAirportLandings',    source_relation=src) }}
        ) as div_airport_landings,
        ds as flight_date_partition,
        metadata$file_last_modified as bronze_loaded_at
    from {{ src }}
    where ds is not null
),

typed as (
    select
        try_to_date(flight_date_raw)                                       as flight_date,
        upper(trim(carrier_code))                                          as carrier_code,
        nullif(trim(flight_number), '')                                    as flight_number,
        nullif(upper(trim(tail_number)), '')                               as tail_number,
        upper(trim(origin_iata))                                           as origin_iata,
        upper(trim(dest_iata))                                             as dest_iata,
        try_to_number(crs_dep_time)                                        as crs_dep_time_hhmm,
        try_to_number(dep_time)                                            as dep_time_hhmm,
        try_to_number(dep_delay_min_raw)::int                              as dep_delay_min,
        try_to_number(crs_arr_time)                                        as crs_arr_time_hhmm,
        try_to_number(arr_time)                                            as arr_time_hhmm,
        try_to_number(arr_delay_min_raw)::int                              as arr_delay_min,
        coalesce(try_to_number(cancelled_raw), 0)::int = 1                 as cancelled_flag,
        nullif(upper(trim(cancellation_code)), '')                         as cancellation_code,
        coalesce(try_to_number(diverted_raw), 0)::int = 1                  as diverted_flag,
        try_to_number(distance_miles_raw)::int                             as distance_miles,
        coalesce(try_to_number(div_airport_landings), 0)::int              as div_airport_landings,
        flight_date_partition,
        bronze_loaded_at
    from raw
)

select
    *,
    -- composite natural key
    md5(
        coalesce(to_varchar(flight_date), '')
        || '|' || coalesce(carrier_code, '')
        || '|' || coalesce(flight_number, '')
        || '|' || coalesce(origin_iata, '')
        || '|' || coalesce(to_varchar(crs_dep_time_hhmm), '')
    ) as flight_event_natural_key
from typed
where flight_date is not null
  and carrier_code is not null
  and flight_number is not null
