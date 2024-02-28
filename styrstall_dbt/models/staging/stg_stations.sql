with source as (
  select
    *
  from {{ source('raw', 'raw_stations') }}
),

final as (
  select 
    station_id,
    name,
    lat,
    long,
    available_bikes,
    date_time,
    bike_ids
  from source
)

select * from final
where lower(name) not like '%bike%'
