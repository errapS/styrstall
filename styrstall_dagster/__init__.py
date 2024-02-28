from dagster import Definitions, load_assets_from_modules
from .assets import stations, weather, dbt_assets
from .resources import database_resource
from .schedules import stations_tenminute_update_schedule, stations_monthly_schedule, weather_daily_update_schedule, weather_monthly_schedule
from .jobs import get_stations_data, stations_summary, get_weather_data, weather_summary

assets = load_assets_from_modules([stations, weather, dbt_assets])

all_jobs = [get_stations_data, stations_summary, get_weather_data, weather_summary]
all_schedules = [stations_tenminute_update_schedule, stations_monthly_schedule, weather_daily_update_schedule, weather_monthly_schedule]

defs = Definitions(
    assets=assets,
    resources={
    	"database": database_resource,  
    },
    jobs=all_jobs,
    schedules=all_schedules,
)
