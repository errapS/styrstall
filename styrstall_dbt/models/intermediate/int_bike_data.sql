with exploded_data as (
  select
    station_id,
    unnest(bike_ids) as bike_id,
    date_time
  from {{ ref('stg_stations') }}
),

final as (
  select
    bike_id,
    station_id,
    date_time
  from exploded_data
  order by bike_id, date_time
)

select * from final
