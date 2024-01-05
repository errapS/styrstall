with source as (
    select
        *
    from {{ ref('trips') }}
),

final as (
    select
        start_station_id as station_id,
        start_lat as lat,
        start_long as long,
        start_time,
        COUNT(*) as total_trips,
        SUM(trip_distance) as total_distance,
        AVG(trip_time_minutes) as avg_trip_time_minutes
    from source
    group by
        start_station_id,
        start_lat,
        start_long,
        start_time
)

select
    station_id,
    lat,
    long,
    start_time,
    total_trips,
    SUM(total_trips) OVER (PARTITION BY station_id ORDER BY start_time) as cumulative_trips,
    total_distance,
    avg_trip_time_minutes
from final
