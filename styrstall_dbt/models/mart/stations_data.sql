with source as (
    select
     *
    from {{ ref('stg_stations') }}
),

final as (
    select
        station_id,
        available_bikes,
        date_time,
        lag(available_bikes) over (partition by station_id order by date_time) as lag_available_bikes
    from source
)

select
    station_id,
    available_bikes,
    coalesce(available_bikes - lag_available_bikes, 0) as bikes_delta,
    date_time
from final
order by station_id, date_time
