{{ config(materialized='incremental') }}

with trips as (
    select
        *
    from {{ ref('int_trips') }}
),

stations as (
    select
        *
    from {{ ref('int_stations') }}
),

trips_with_distance as (
    select 
        s.name as station_name,
        t.time_delta as time_delta,
        {{ haversine_distance('s.lat', 's.long', 's.close', 's.long_2') }} as distance
    from trips as t
    join stations as s on t.start_station = s.station_id_1  
),
 

final as (
    select
        station_name as station_name,
        COUNT(*) as total_trips,
        AVG(distance) as avg_trip_distance,
        AVG(trip_time_minutes) as avg_trip_time_minutes
    from trips_with_distance
    group by
        station_name
)

select
    station_name,
    total_trips,
    avg_trip_distance,
    avg_trip_time_minutes
from final
