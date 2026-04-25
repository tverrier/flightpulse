{# A flight that has already landed cannot have an actual_arr_ts in the
   future of the dbt run. If we see one, it's a tz parsing bug. #}

select
    flight_event_sk,
    actual_arr_ts,
    current_timestamp() as observed_at
from {{ ref('fct_flight_event') }}
where actual_arr_ts is not null
  and actual_arr_ts > current_timestamp()
