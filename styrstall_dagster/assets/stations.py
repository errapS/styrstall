from dagster import AssetExecutionContext, MetadataValue, asset
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource, dbt_assets
from azure.storage.blob import BlobServiceClient
from pathlib import Path
import os, json
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import BytesIO
from . import constants
from .helpers import convert_timestamp, upload_to_blob_storage, read_files_from_blob, read_single_blob
# from ..partitions import monthly_partition


@asset(
    group_name="ingestion"
)
def raw_stations_blob():

    api_key = os.getenv('API_KEY')
    raw_stations = requests.get(
        f"https://data.goteborg.se/SelfServiceBicycleService/v2.0/Stations/{api_key}?format=json"
    ).json()

    last_update = convert_timestamp(raw_stations[0].get("LastUpdate", "")).strftime('%Y%m%d_%H%M')
    file_name = constants.RAW_STATIONS_TEMPLATE_FILE_NAME.format(last_update)
    data_str = json.dumps(raw_stations)

    upload_to_blob_storage(data_str, file_name, container_name='raw', blob_prefix=f"stations/{last_update[:6]}")
    

@asset(
    deps=["raw_stations_blob"],
    group_name="ingestion",
)
def raw_stations_parquet():

    # month = (datetime.utcnow() - relativedelta(months=1)).strftime('%Y%m')
    month = datetime.utcnow().strftime('%Y%m')

    combined_df = read_files_from_blob(container_name='raw', blob_prefix=f"stations/{month}/stations")
    combined_df['LastUpdate'] = combined_df['LastUpdate'].apply(convert_timestamp)

    parquet_buffer = BytesIO()
    combined_df.to_parquet(parquet_buffer, index=False)

    parquet_buffer.seek(0)
    file_name = constants.STG_STATIONS_TEMPLATE_FILE_NAME.format(month)
    upload_to_blob_storage(parquet_buffer.read(), file_name, container_name='stg', blob_prefix=f"stations")
    

@asset(
    deps=["raw_stations_parquet"],
    # partitions_def=monthly_partition,
    group_name="raw", 
)
def raw_stations(database: DuckDBResource):
    """
      The raw stations dataset, loaded into a DuckDB database, partitioned by month.
    """

    # FIX CERT ISSUE https://github.com/duckdb/duckdb_azure/issues/8
    # LOAD azure;
    # SET azure_storage_connection_string = '{os.getenv('AZURE_STORAGE_CONNECTION_STRING')}';
    # 'azure://stg/stations/stations_{month_to_fetch}.parquet'
    # partition_date_str = context.asset_partition_key_for_output()
    
    # month_to_fetch = partition_date_str[:-3]

    month_to_fetch = '202401'
    parquet_df = read_single_blob('stg', f"stations/{constants.STG_STATIONS_TEMPLATE_FILE_NAME.format(month_to_fetch)}")


    query = f"""
        CREATE TABLE IF NOT EXISTS raw_stations (
            station_id INTEGER,
            name VARCHAR,
            lat DOUBLE,
            long DOUBLE,
            is_open BOOLEAN,
            available_bikes INTEGER,
            date_time TIMESTAMP,
            bike_ids VARCHAR[],
            partition_date VARCHAR
        );

        DELETE FROM raw_stations WHERE partition_date = '202311';

        INSERT INTO raw_stations
        SELECT
            CAST(StationID AS INTEGER) AS station_id,
            CAST(Name AS VARCHAR) AS name,
            CAST(Lat AS DOUBLE) AS lat,
            CAST(Long AS DOUBLE) AS long,
            CAST(IsOpen AS BOOLEAN) AS is_open,
            CAST(AvailableBikes AS INTEGER) AS available_bikes,
            CAST(LastUpdate AS TIMESTAMP) AS date_time,
            CAST(BikeIds AS VARCHAR) AS bike_ids,
            '{month_to_fetch}' AS partition_date
        FROM
            parquet_df
    """

    with database.get_connection() as conn:
      conn.execute(query)

