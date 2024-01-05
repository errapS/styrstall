from datetime import datetime, timedelta, timezone
from azure.storage.blob import BlobServiceClient
import os, json
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO

def convert_timestamp(timestamp_str):
    timestamp_str = timestamp_str.split('(')[1].split(')')[0]
    timestamp = int(timestamp_str[:-5])
    offset_str = timestamp_str[-5:]
    
    dt = datetime.utcfromtimestamp(timestamp / 1000)
    
    offset = timedelta(hours=int(offset_str[:3]), minutes=int(offset_str[3:]))
    dt_with_offset = dt + offset
    
    return dt_with_offset


def upload_to_blob_storage(data, file_name, container_name, blob_prefix=""):
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{blob_prefix}/{file_name}")

    blob_client.upload_blob(data)


def read_files_from_blob(container_name, blob_prefix):
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    all_blobs = container_client.walk_blobs(name_starts_with=blob_prefix)
    
    df_list = []

    
    for blob in all_blobs:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob.name)
        blob_data = blob_client.download_blob()
        
        if 'weather' in blob_prefix:
            data = json.loads(blob_data.readall())['data']
            
            for i in range(len(data)):
                row = pd.DataFrame(data[i]['observations'])
                row['referenceTime'] = data[i]['referenceTime']
                row['sourceId'] = data[i]['sourceId']
                df_list.append(row)
        else:
            data = json.loads(blob_data.readall())
            df = pd.json_normalize(data)
            df_list.append(df)

    return pd.concat(df_list, ignore_index=True)


def read_single_blob(container_name, blob_path):
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    blob_data = blob_client.download_blob()

    # parquet_file = pq.ParquetFile(BytesIO(blob_data.readall()))

    df = pd.read_parquet(BytesIO(blob_data.readall()))

    return df
    # table = parquet_file.read_row_group(0).to_pandas()

    # # Convert the pandas DataFrame to a dictionary
    # data = table.to_dict(orient='records')

    # return data