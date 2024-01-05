with fs as (
    select
        *
    from {{ ref('stations') }}
),

db as (
    select
        *
    from {{ ref('bike_data') }}
),

trips as (
    select
        db.bike_id,
        db.station_id as end_station_id,
        fs.name as end_station_name,
        fs.lat as end_lat,
        fs.long as end_long,
        db.date_time as end_time,
        LAG(db.station_id) over (partition by db.bike_id order by db.date_time) as start_station_id,
        LAG(fs.name) over (partition by db.bike_id order by db.date_time) as start_station_name,
        LAG(fs.lat) over (partition by db.bike_id order by db.date_time) as start_lat,
        LAG(fs.long) over (partition by db.bike_id order by db.date_time) as start_long,
        LAG(db.date_time) over (partition by db.bike_id order by db.date_time) as start_time
    from
        db
        join fs on db.station_id = fs.station_id
    order by 
        db.bike_id, db.date_time 
)

select
    t.bike_id,
    t.start_station_id,
    t.start_station_name,
    t.start_lat,
    t.start_long,
    t.start_time,
    t.end_station_id,
    t.end_station_name,
    t.end_lat,
    t.end_long,
    t.end_time,
    {{ haversine_distance('t.start_lat', 't.start_long', 't.end_lat', 't.end_long') }} as trip_distance,
    date_diff('minute', t.start_time, t.end_time) as trip_time_minutes,
    dayofweek(t.start_time) as dayofweek
from
    trips t
where
    t.end_time is not null
    and start_station_id <> end_station_id
order by 
    t.bike_id, t.start_time
