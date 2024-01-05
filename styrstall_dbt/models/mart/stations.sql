with source as (
    select
        *
    from {{ ref('stg_stations') }}
),

final as (
    select
        a.station_id as station_id_1,
        a.name as name_1,
        a.lat as lat_1,
        a.long as long_1,
        b.station_id as station_id_2,
        b.name as name_2,
        b.lat as lat_2,
        b.long as long_2,
        {{ haversine_distance('a.lat', 'a.long', 'b.lat', 'b.long') }} as distance
    from source a
    cross join source b
    where a.station_id != b.station_id
)

select
    station_id_1 as station_id,
    name_1 as name,
    lat_1 as lat,
    long_1 as long,
    station_id_2 as closest_station_id,
    name_2 as closest_station_name,
    lat_2 as closest_station_lat,
    long_2 as closest_station_long,
    distance
from (
    select
        station_id_1,
        name_1,
        lat_1,
        long_1,
        station_id_2,
        name_2,
        lat_2,
        long_2,
        distance,
        ROW_NUMBER() over (partition by station_id_1 order by distance) as rn
    from final
) where rn = 1