import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from argparse import ArgumentParser
from utils.common_utils import unzip_file,read_params


def data_upload(container_client, local_folder,raw_data,raw_zip_data):
    
    unzip_file(raw_zip_data,raw_data)

    try:
        container_client.create_container()
    except Exception as e:
        print(f"Container already exists: {e}")
    
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder)
            blob_path = relative_path.replace("\\", "/")

            print(f"Uploading {local_file_path} to {blob_path}...")

            blob_client = container_client.get_blob_client(blob_path)
            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

if __name__=='__main__':
    args=ArgumentParser()
    args.add_argument("--config_path", '-c', default='params.yaml')
    parsed_args=args.parse_args()
    configs=read_params(parsed_args.config_path)

    connect_str = configs['container']['connect_str']
    container_name = configs['container']['container_name']
    local_folder = configs['data_dir']['artifacts']
    raw_zip_data=configs['data_dir']['raw_zip_data']
    raw_data=configs['data_dir']['raw_data']

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    

    data_upload(container_client, local_folder,raw_data,raw_zip_data)