import os
import requests
import json
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient


RAW_STATIONS_TEMPLATE_FILE_NAME = "stations_{}.json"
CONNECTION_STRING_TEMPLATE = "DefaultEndpointsProtocol=https;AccountName=styrstallstorage;AccountKey={}==;EndpointSuffix=core.windows.net

def convert_timestamp(timestamp_str):
    timestamp_str = timestamp_str.split('(')[1].split(')')[0]
    timestamp = int(timestamp_str[:-5])
    offset_str = timestamp_str[-5:]
    
    dt = datetime.utcfromtimestamp(timestamp / 1000)
    
    offset = timedelta(hours=int(offset_str[:3]), minutes=int(offset_str[3:]))
    dt_with_offset = dt + offset
    
    return dt_with_offset


def upload_to_blob_storage(data, file_name, container_name, blob_prefix=""):
    connection_string = CONNECTION_STRING_TEMPLATE.format(os.getenv('AZURE_STORAGE_CONNECTION_STRING'))
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{blob_prefix}/{file_name}")

    blob_client.upload_blob(data)


def raw_stations_blob():
    api_key = os.getenv('STYRSTALL_API_KEY')
    raw_stations = requests.get(
        f"https://data.goteborg.se/SelfServiceBicycleService/v2.0/Stations/{api_key}?format=json"
    ).json()

    last_update = convert_timestamp(raw_stations[0].get("LastUpdate", "")).strftime('%Y%m%d_%H%M')
    file_name = RAW_STATIONS_TEMPLATE_FILE_NAME.format(last_update)
    data_str = json.dumps(raw_stations)

    upload_to_blob_storage(data_str, file_name, container_name='raw', blob_prefix=f"stations/{last_update[:6]}")


if __name__ == "__main__":
    raw_stations_blob()
