with bike as (
    select
        *
    from {{ ref('int_bike_data') }}
),

trips as (
    select
        bike_id,
        station_id as start_station_id,
        date_time as start_time,
        LEAD(station_id) OVER (PARTITION BY bike_id ORDER BY date_time) AS end_station_id,
        LEAD(date_time) OVER (PARTITION BY bike_id ORDER BY date_time) AS end_time
    from bike 
),

trip_data as (
    select
        t.bike_id,
        t.start_station_id,
        t.start_time,
        t.end_station_id,
        t.end_time,
        date_diff('minute', t.start_time, t.end_time) as time_delta
    from
        trips t
)

select
    bike_id,
    start_station_id,
    start_time,
    end_station_id,
    end_time,
    time_delta
from
    trip_data
where 
    start_station_id <> end_station_id
-- filter on trip time to remove false trips...
