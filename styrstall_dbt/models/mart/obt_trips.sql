{{ config(materialized='incremental') }}

with trips as (
  select 
    *
  from {{ ref('int_trips') }}
  where
    start_time >= cast('{{ var("min_date") }}' as timestamp)
  and
    start_time < cast('{{ var("max_date") }}' as timestamp)
),

stations as (
  select 
    *
  from {{ ref('int_stations') }}
),

final as (
  select 
    t.bike_id,
    s.name as start_station_name,
    t.start_time,
    e.name as end_station_name, 
    t.end_time,
    t.time_delta,
    {{ haversine_distance('s.lat', 's.long', 'e.lat', 'e.long') }} as distance
  from 
    trips t 
  join
    stations s on t.start_station_id = s.station_id
  join 
    stations e on t.end_station_id = e.station_id
)

select * from final 
