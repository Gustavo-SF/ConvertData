import logging
import os
import sys
from collections import defaultdict

import azure.functions as func
from azure.storage.blob import BlobServiceClient

from .settings import LogMessages as LOGS
from .process import ProcessingPipeline

logging.getLogger("requests").setLevel(logging.WARNING)


def main(msg: func.QueueMessage):
    """Code to be executed when trigger is set"""

    message = msg.get_body().decode("utf-8")
    logging.info(LOGS.initial.format(msg=message))

    connection_string = os.getenv("AzureWebJobsStorage")

    optimization_for_gets_and_puts = {
        "max_single_put_size": 4 * 1024 * 1024,  # split to 4MB chunks`
        "max_single_get_size": 4 * 1024 * 1024,  # split to 4MB chunks
    }

    blob_client = BlobServiceClient.from_connection_string(
        conn_str=connection_string, **optimization_for_gets_and_puts
    )

    # Get the transactions we CAN process from deployment or maintenance
    blob_container_client = blob_client.get_container_client(container=message)
    blob_list = [blob.name for blob in blob_container_client.list_blobs()]
    transactions_to_process = defaultdict(list)
    for file in blob_list:
        if len(file.split("/")) > 1:
            transactions_to_process[file.split("/")[0]].append(file)

    # Get the files we already have processed
    blob_processed_container_list = blob_client.get_container_client(
        container="data-ready"
    )
    done_blobs = [blob.name[:-4] for blob in blob_processed_container_list.list_blobs()]

    for transaction in transactions_to_process:
        if not transaction in done_blobs:
            files = transactions_to_process[transaction]
            pipeline = ProcessingPipeline(
                transaction, files, blob_client, blob_container_client
            )
            pipeline.transform()
            pipeline.upload()
        logging.info(LOGS.finished_processing.format(msg=transaction))
