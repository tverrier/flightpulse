{# The aggregate must reconcile to the detail. For each (flight_date,
   carrier_key), agg.flights_scheduled MUST equal the detail count.
   Tolerance is zero on a fresh run; the dbt schedule is daily so any
   drift indicates an aggregation bug or a partial backfill. #}

with detail as (
    select flight_date_key as flight_date,
           carrier_key,
           count(*) as detail_count
    from {{ ref('fct_flight_event') }}
    group by 1, 2
),

agg as (
    select flight_date,
           carrier_key,
           flights_scheduled
    from {{ ref('agg_carrier_otp_daily') }}
)

select
    coalesce(d.flight_date, a.flight_date) as flight_date,
    coalesce(d.carrier_key, a.carrier_key) as carrier_key,
    d.detail_count,
    a.flights_scheduled
from detail d
full outer join agg a using (flight_date, carrier_key)
where coalesce(d.detail_count, -1) <> coalesce(a.flights_scheduled, -1)
