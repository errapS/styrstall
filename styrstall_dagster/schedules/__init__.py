from dagster import ScheduleDefinition
from ..jobs import *

stations_tenminute_update_schedule = ScheduleDefinition(
    job=get_stations_data,
    cron_schedule="*/10 * * * *", # every ten minutes
    execution_timezone="CET",
)

stations_monthly_schedule = ScheduleDefinition(
    job=stations_summary,
    cron_schedule="5 0 1 * *", # 00:05 1st every month
    execution_timezone="CET",
)

weather_daily_update_schedule = ScheduleDefinition(
    job=get_weather_data,
    cron_schedule="0 1 * * *", # 00:00 daily
    execution_timezone="CET",
)

weather_monthly_schedule= ScheduleDefinition(
    job=weather_summary,
    cron_schedule="5 0 1 * *", # 00:05 1st every month
    execution_timezone="CET",
)
