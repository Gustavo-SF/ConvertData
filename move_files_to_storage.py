"""
Module to move files from local to storage
"""
import sys
import json
from pathlib import Path
import os

from azure.storage.blob import ContainerClient, BlobServiceClient


container = sys.argv[1]
con_string = json.load(open(Path(".") / "local.settings.json", "r"))["Values"]["AzureWebJobsStorage"]
optimization_for_gets_and_puts = {
    "max_single_put_size": 4 * 1024 * 1024,  # split to 4MB chunks`
    "max_single_get_size": 4 * 1024 * 1024,  # split to 4MB chunks
}
container_client = ContainerClient.from_connection_string(conn_str=con_string, container_name=container, **optimization_for_gets_and_puts)

data_path = Path(".") / "data" / container
local_data_files = data_path.glob("*/*")

files = [file.relative_to(data_path) for file in local_data_files]

if __name__ == "__main__":
    for file in files:
        blob = container_client.get_blob_client(blob=str(file))
        with open(data_path / file, "rb") as data:
            blob.upload_blob(data, overwrite=True)
        print(f"[PPP] Uploaded {file} into {container}.")

    if container == "maintenance":
        blob_service_client = BlobServiceClient.from_connection_string(con_string)
        for file in files:
            source_blob = (f"https://{os.getenv('STORAGE_ACCOUNT')}.blob.core.windows.net/maintenance/{str(file)}")

            # Target
            copied_blob = blob_service_client.get_blob_client("deployment", str(file))
            copied_blob.start_copy_from_url(source_blob)

            print(f"[PPP] Copied {file} to deployment container")

# Files need to be removed from maintenance and data-ready as well.