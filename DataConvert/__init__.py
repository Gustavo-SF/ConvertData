import logging
import os
import sys
from collections import defaultdict

import azure.functions as func
from azure.storage.blob import ContainerClient

from .settings import LogMessages as LOGS
from .process import ProcessingPipeline


filefmt = "%(levelname)s - %(asctime)s - %(message)s"
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=filefmt)

def main(msg: func.QueueMessage):
    """Code to be executed when trigger is set"""

    message = msg.get_body().decode('utf-8')
    logging.info(LOGS.initial.format(msg=message))

    connection_string = os.getenv("AzureWebJobsStorage")

    blob_container_client = ContainerClient.from_connection_string(conn_str=connection_string, container_name=message)
    blob_list = [blob.name for blob in blob_container_client.list_blobs()]

    transactions_to_process = defaultdict(list)
    for file in blob_list:
        if len(file.split("/")) > 1:
            transactions_to_process[file.split("/")[0]].append(file)

    for transaction in transactions_to_process:
        if transaction == "mb52":
            files = transactions_to_process[transaction]
            pipeline = ProcessingPipeline(transaction, files, blob_container_client)
            pipeline.transform()
            pipeline.upload()

    logging.info(LOGS.finished_processing.format(msg=message))