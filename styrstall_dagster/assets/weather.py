from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path
import os, json
import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from io import BytesIO
from . import constants
from .helpers import upload_to_blob_storage, read_files_from_blob, read_single_blob


@asset(
    group_name="ingestion"
)
def raw_weather_blob():
    client_id = os.getenv('WEATHER_API_KEY')
    api_url = "https://frost.met.no/observations/v0.jsonld"

    today = datetime.utcnow().strftime('%Y-%m-%d')
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    parameters = {
        'sources': 'SN251300',
        'elements': 'air_temperature, wind_speed, max(wind_speed_of_gust PT1H), wind_from_direction, sum(precipitation_amount PT1H), air_pressure_at_sea_level, relative_humidity',
        'referencetime': f'{yesterday}/{today}',
    }

    raw_weather = requests.get(api_url, parameters, auth=(client_id,'')).json()

    last_update = yesterday.replace('-', '')
    file_name = constants.RAW_WEATHER_TEMPLATE_FILE_NAME.format(last_update)
    data_str = json.dumps(raw_weather)

    upload_to_blob_storage(data_str, file_name, container_name='raw', blob_prefix=f"weather/{last_update[:6]}")


@asset(
    deps=["raw_weather_blob"],
    group_name="ingestion",
)
def raw_weather_parquet():
    # month = (datetime.utcnow() - relativedelta(months=1)).strftime('%Y%m')
    month = datetime.utcnow().strftime('%Y%m')

    blob_prefix = f"weather/{month}/weather"
    df = read_files_from_blob('raw', blob_prefix)

    df = df.reset_index()

    columns = ['sourceId','referenceTime','elementId','value','unit','timeOffset']
    df2 = df[columns].copy()

    df2['referenceTime'] = pd.to_datetime(df2['referenceTime'])

    parquet_buffer = BytesIO()
    df2.to_parquet(parquet_buffer, index=False)

    parquet_buffer.seek(0)
    file_name = constants.STG_WEATHER_TEMPLATE_FILE_NAME.format(month)
    upload_to_blob_storage(parquet_buffer.read(), file_name, container_name='stg', blob_prefix=f"weather")


@asset(
    deps=["raw_weather_parquet"],
    # partitions_def=monthly_partition,
    group_name="raw", 
)
def raw_weather(database: DuckDBResource):
    """
      The raw weather dataset, loaded into a DuckDB database, partitioned by month.
    """

    # partition_date_str = context.asset_partition_key_for_output()
    # month_to_fetch = partition_date_str[:-3]

    month_to_fetch = '202401'
    parquet_df = read_single_blob('stg', f"weather/{constants.STG_WEATHER_TEMPLATE_FILE_NAME.format(month_to_fetch)}")

    # FIX CERT ISSUE https://github.com/duckdb/duckdb_azure/issues/8
    # LOAD azure;
    # SET azure_storage_connection_string = '{os.getenv('AZURE_STORAGE_CONNECTION_STRING')}';
    # 'azure://stg/stations/stations_{month_to_fetch}.parquet'

    query = f"""
        CREATE TABLE IF NOT EXISTS raw_weather (
            source_id VARCHAR,
            reference_time TIMESTAMP,
            element VARCHAR,
            value DOUBLE, 
            unit VARCHAR,
            time_offset VARCHAR,
            partition_date VARCHAR
        );

        DELETE FROM raw_weather WHERE partition_date = '{month_to_fetch}';

        INSERT INTO raw_weather
        SELECT
            CAST(sourceId AS VARCHAR) AS source_id,
            CAST(referenceTime AS  TIMESTAMP) AS reference_time,
            CAST(elementId AS VARCHAR) AS element,
            CAST(value AS DOUBLE),
            CAST(unit AS VARCHAR),
            CAST(timeOffset AS VARCHAR) AS time_offset,
            '{month_to_fetch}' AS partition_date
        FROM
            parquet_df
    """

    with database.get_connection() as conn:
      conn.execute(query)
    
