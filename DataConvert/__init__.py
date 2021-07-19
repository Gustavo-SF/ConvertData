import logging
import os

import azure.functions as func
from azure.storage.queue import (
    QueueClient, 
    TextBase64EncodePolicy
)

from .settings import Files, LogMessages as LOGS
from .process import *


def send_message(msg: str, connect_str: str, queue_name: str) -> None:
    """Send message back to the Queue to trigger it again"""

    base64_queue_client = QueueClient.from_connection_string(
                    conn_str = connect_str,
                    queue_name = queue_name,
                    message_encode_policy = TextBase64EncodePolicy()
                )
    base64_queue_client.send_message(msg)


def main(msg: func.QueueMessage, output: func.Out[func.InputStream]):
    """Code to be executed when trigger is set"""

    message = msg.get_body().decode('utf-8')
    logging.info(LOGS.initial.format(msg=message))

    connection_string = os.getenv("AzureWebJobsStorage")
    container_name = "raw-data"

    if message == "all":
        for new_msg in Files.all():
            send_message(new_msg, connection_string, "uploadedfiles")

    else:
        if message in Files.all():
            if message in Files.text():
                output_csv = process_text_file(message, connection_string, container_name)
            elif message in Files.xlsx:
                output_csv = process_xlsx_file(message, connection_string, container_name)
            elif message in Files.xlsx_folder:
                output_csv = process_xlsx_folder(message, connection_string, container_name, subdir=f"{message}/")
            output.set(output_csv)
            logging.info(LOGS.csv_creation_success.format(msg=message))
        else:
            logging.info(LOGS.no_command_defined.format(msg=message))