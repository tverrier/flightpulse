# Data Model

## ERD

```
                ┌──────────────┐
                │  dim_date    │
                └──────┬───────┘
                       │ flight_date_key
┌──────────────┐       │       ┌──────────────┐
│ dim_carrier  │───────┤       │  dim_airport │
└──────┬───────┘       │       └──────┬───────┘
       │ carrier_key   │              │ origin_airport_key
       │               │              │ dest_airport_key
       │       ┌───────▼──────────────▼──────────────┐
       └──────►│        fct_flight_event             │◄──────┐
               │  grain: 1 row / scheduled flight    │       │
               └─────────────────────┬───────────────┘       │
                                     │ aircraft_key          │
                                     ▼                       │
                              ┌──────────────┐               │
                              │ dim_aircraft │               │
                              └──────────────┘               │
                                                             │
                                  agg_carrier_otp_daily ─────┘
```

## Tables

### `gold.fct_flight_event` (FACT — transaction)

| Column | Type | Null | Description | Example |
|---|---|---|---|---|
| flight_event_sk | STRING | N | md5 surrogate of natural key | `9a3f...` |
| flight_date_key | DATE | N | FK → dim_date | `2024-01-15` |
| carrier_key | STRING | N | FK → dim_carrier | `WN` |
| origin_airport_key | STRING | N | FK → dim_airport (IATA) | `DEN` |
| dest_airport_key | STRING | N | FK → dim_airport | `LAX` |
| aircraft_key | STRING | Y | FK → dim_aircraft (tail num) | `N8602F` |
| flight_number | STRING | N | Carrier flight number | `1234` |
| scheduled_dep_ts | TIMESTAMP_TZ | N | Scheduled departure | `2024-01-15 08:30:00 -07` |
| actual_dep_ts | TIMESTAMP_TZ | Y | NULL if cancelled | |
| scheduled_arr_ts | TIMESTAMP_TZ | N | | |
| actual_arr_ts | TIMESTAMP_TZ | Y | NULL if cancelled or in-flight | |
| dep_delay_min | INTEGER | Y | actual − scheduled | `12` |
| arr_delay_min | INTEGER | Y | | `5` |
| cancelled_flag | BOOLEAN | N | | `false` |
| cancellation_code | STRING | Y | A=carrier, B=weather, C=NAS, D=security | `B` |
| diverted_flag | BOOLEAN | N | | `false` |
| distance_miles | INTEGER | N | Great-circle | `863` |
| live_actual_arr_ts | TIMESTAMP_TZ | Y | OpenSky landing detection (provisional) | |
| reconciliation_status | STRING | N | `live_only` / `bts_only` / `matched` / `mismatch` | `matched` |
| source_system | STRING | N | `bts` / `opensky_reconciled` | `bts` |
| ingested_at | TIMESTAMP_TZ | N | dbt run timestamp | |
| record_hash | STRING | N | sha256 of source columns; CDC marker | |

### `gold.dim_carrier` (DIM, SCD-1)

| Column | Type | Null | Description | Example |
|---|---|---|---|---|
| carrier_key | STRING | N | IATA code (PK) | `WN` |
| carrier_name | STRING | N | Full name | `Southwest Airlines` |
| icao_code | STRING | Y | | `SWA` |
| country | STRING | N | | `United States` |
| is_active | BOOLEAN | N | | `true` |
| valid_from | DATE | N | Earliest seen | `1987-01-01` |
| valid_to | DATE | Y | NULL = current | NULL |

### `gold.dim_airport` (DIM, SCD-2)

| Column | Type | Null | Description | Example |
|---|---|---|---|---|
| airport_sk | STRING | N | Surrogate (IATA + valid_from hash) | `9b...` |
| airport_key | STRING | N | IATA code (business key) | `DEN` |
| icao_code | STRING | Y | | `KDEN` |
| name | STRING | N | | `Denver Intl` |
| city | STRING | N | | `Denver` |
| state | STRING | Y | | `CO` |
| country | STRING | N | | `US` |
| latitude | FLOAT | N | | `39.8617` |
| longitude | FLOAT | N | | `-104.6731` |
| elevation_ft | INTEGER | Y | | `5431` |
| timezone | STRING | N | | `America/Denver` |
| valid_from | DATE | N | | |
| valid_to | DATE | Y | | |
| is_current | BOOLEAN | N | | |

### `gold.dim_aircraft` (DIM, SCD-1)

| Column | Type | Null | Description | Example |
|---|---|---|---|---|
| aircraft_key | STRING | N | Tail number (PK) | `N8602F` |
| icao24 | STRING | Y | ADS-B 24-bit address | `a4d8b9` |
| manufacturer | STRING | Y | | `Boeing` |
| model | STRING | Y | | `737-800` |
| operator_carrier_key | STRING | Y | FK → dim_carrier | `WN` |
| year_built | INTEGER | Y | | `2014` |
| seat_capacity | INTEGER | Y | | `175` |

### `gold.dim_date` (DIM, static)

Generated via `dbt-utils.date_spine`. Columns: `date_key, year, quarter, month, day, day_of_week, day_name, is_weekend, is_us_federal_holiday, fiscal_year, fiscal_quarter, iso_week`.

### `gold.agg_carrier_otp_daily` (FACT — aggregate)

| Column | Type | Description |
|---|---|---|
| flight_date | DATE | |
| carrier_key | STRING | |
| flights_scheduled | INTEGER | |
| flights_completed | INTEGER | |
| flights_cancelled | INTEGER | |
| on_time_pct | FLOAT | arrivals within 14 min |
| avg_arr_delay_min | FLOAT | |
| total_delay_min | INTEGER | |

## Fact / Dim Classification

| Table | Type | Grain | Reasoning |
|---|---|---|---|
| `fct_flight_event` | Transaction fact | 1 row per scheduled flight | Lowest atomic event |
| `agg_carrier_otp_daily` | Aggregate fact | 1 row per (carrier, date) | Pre-aggregated for dashboard speed |
| `dim_date` | Conformed dim | 1 row per date | Reused everywhere |
| `dim_carrier` | Type-1 dim | 1 row per carrier | Carriers rarely change attributes |
| `dim_airport` | Type-2 dim | 1 row per airport per attribute change | Renames / relocations matter for old flights |
| `dim_aircraft` | Type-1 dim | 1 row per tail number | Operator changes tracked elsewhere |

## dbt Model DAG

```
sources:
  bts_raw                ─► stg_bts__flights
  opensky_raw            ─► stg_opensky__states
  openflights_airports   ─► stg_openflights__airports
  openflights_airlines   ─► stg_openflights__airlines

intermediate:
  stg_bts__flights        ─► int_flight_event_bts
  stg_opensky__states     ─► int_aircraft_state_clean ─► int_landing_events
  int_flight_event_bts + int_landing_events
                          ─► int_flight_event_reconciled

marts (gold):
  int_flight_event_reconciled       ─► fct_flight_event
  stg_openflights__airports         ─► dim_airport (snapshot, SCD-2)
  stg_openflights__airlines         ─► dim_carrier
  distinct(stg_bts.tail_num)        ─► dim_aircraft
  date_spine seed                   ─► dim_date
  fct_flight_event                  ─► agg_carrier_otp_daily
```

## dbt Tests

| Test | On | Catches | Severity |
|---|---|---|---|
| `not_null` | `flight_event_sk`, `flight_date_key`, `carrier_key`, `origin_airport_key`, `dest_airport_key` | Broken FK or surrogate logic | error |
| `unique` | `flight_event_sk` | Bad dedupe / merge | error |
| `relationships` | every FK → its dim | Late-arriving dim, typo | error |
| `accepted_values` | `cancellation_code` in {A,B,C,D,NULL} | Source schema change | warn |
| `accepted_values` | `reconciliation_status` in {live_only,bts_only,matched,mismatch} | Reconciliation logic break | error |
| `dbt_utils.expression_is_true` | `arr_delay_min = datediff(min, scheduled_arr_ts, actual_arr_ts)` for non-cancelled | Math regression | error |
| `dbt_utils.expression_is_true` | `actual_dep_ts is null` when `cancelled_flag` | Bad cancellation handling | error |
| `dbt_utils.recency` | `fct_flight_event.ingested_at` within 25h | Pipeline stalled | error |
| `dbt_expectations.expect_column_value_lengths_to_equal` | `carrier_key` length=2 | Source code field misuse | warn |
| `dbt_expectations.expect_column_quantile_values_to_be_between` | `arr_delay_min` 95p between -30 and 240 | Outlier explosion = upstream bug | warn |
| custom singular | `assert_no_future_actual_arrivals.sql` | Time-zone bug | error |
| custom singular | `assert_carrier_otp_sums.sql` | Aggregate ≠ detail | error |

All `error`-severity tests are tagged `critical`; CI gate runs only those on PR for fast feedback.

## Lineage Map

| Source field | Bronze | Silver | Gold |
|---|---|---|---|
| `BTS.FlightDate` | `bts_raw.FlightDate` (string) | `silver.flight_event.flight_date` (date) | `fct_flight_event.flight_date_key` |
| `BTS.Reporting_Airline` | `bts_raw.Reporting_Airline` | `silver.flight_event.carrier_code` | `fct_flight_event.carrier_key` + `dim_carrier.carrier_key` |
| `BTS.Origin` | `bts_raw.Origin` | `silver.flight_event.origin_iata` | `fct_flight_event.origin_airport_key` |
| `BTS.Dest` | `bts_raw.Dest` | `silver.flight_event.dest_iata` | `fct_flight_event.dest_airport_key` |
| `BTS.ArrDelay` | `bts_raw.ArrDelay` | `silver.flight_event.arr_delay_min` | `fct_flight_event.arr_delay_min` |
| `BTS.DepDelay` | `bts_raw.DepDelay` | `silver.flight_event.dep_delay_min` | `fct_flight_event.dep_delay_min` |
| `BTS.Cancelled` | `bts_raw.Cancelled` ('0'/'1') | `silver.flight_event.cancelled_flag` (bool) | `fct_flight_event.cancelled_flag` |
| `BTS.CancellationCode` | `bts_raw.CancellationCode` | `silver.flight_event.cancellation_code` | `fct_flight_event.cancellation_code` |
| `BTS.Tail_Number` | `bts_raw.Tail_Number` | `silver.flight_event.tail_number` | `dim_aircraft.aircraft_key`, `fct_flight_event.aircraft_key` |
| `BTS.Distance` | `bts_raw.Distance` | `silver.flight_event.distance_miles` | `fct_flight_event.distance_miles` |
| `OpenSky.icao24` | `opensky_raw.icao24` | `silver.aircraft_state.icao24` | `dim_aircraft.icao24`, joined to `fct_flight_event.aircraft_key` via tail num |
| `OpenSky.time_position` | `opensky_raw.time_position` | `silver.aircraft_state.event_ts` | `fct_flight_event.live_actual_arr_ts` (when landing detected) |
| `OpenSky.on_ground` | `opensky_raw.on_ground` | `silver.aircraft_state.on_ground` | landing detection input only (not in gold) |
| `OpenFlights.iata` | `openflights_airports.iata` | `silver.dim_airport_seed.iata` | `dim_airport.airport_key` |
| `OpenFlights.tz` | `openflights_airports.tz` | `silver.dim_airport_seed.tz` | `dim_airport.timezone` |
| `OpenFlights.airline_iata` | `openflights_airlines.iata` | `silver.dim_airline_seed.iata` | `dim_carrier.carrier_key` |

`dbt docs serve` renders the live graph; this table covers the resume-relevant 80%.
