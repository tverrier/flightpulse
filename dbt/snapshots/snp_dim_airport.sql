{# SCD-2 snapshot for airports. Materializes valid_from / valid_to for the
   dim_airport model upstream. Strategy=check on the descriptive cols so a
   rename or coordinate move opens a new version. #}

{% snapshot snp_dim_airport %}

    {{
        config(
            target_schema='snapshots',
            unique_key='iata',
            strategy='check',
            check_cols=['name', 'city', 'country', 'icao',
                        'latitude', 'longitude', 'elevation_ft', 'timezone'],
            invalidate_hard_deletes=True,
        )
    }}

    select
        iata,
        icao,
        name,
        city,
        country,
        latitude,
        longitude,
        elevation_ft,
        timezone
    from {{ ref('stg_openflights__airports') }}
    where iata is not null

{% endsnapshot %}
