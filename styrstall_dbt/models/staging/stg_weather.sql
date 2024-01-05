with source as (
  select
    *
  from {{ source('raw', 'raw_weather') }}
),

cleaned_source as (
  select
    reference_time,
    element,
    case
      when element = 'sum(precipitation_amount PT1H)' and value = -1 then 0
      else value
    end as value
  from source
)

select *
from cleaned_source
pivot (
  sum(value)
  for element in (
    'air_temperature' as air_temperature,
    'wind_speed' as wind_speed,
    'max(wind_speed_of_gust PT1H)' as max_wind_speed_of_gust,
    'wind_from_direction' as wind_from_direction,
    'sum(precipitation_amount PT1H)' as precipitation_amount,
    'air_pressure_at_sea_level' as air_pressure_at_sea_level,
    'relative_humidity' as relative_humidity
  )
) order by reference_time


