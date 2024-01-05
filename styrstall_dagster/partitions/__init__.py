from dagster import MonthlyPartitionsDefinition, WeeklyPartitionsDefinition
from datetime import datetime
from ..assets import constants

start_date = constants.START_DATE

monthly_partition = MonthlyPartitionsDefinition(
  start_date=start_date,
  end_date=datetime.utcnow().strftime('%Y-%m-%d')
)
