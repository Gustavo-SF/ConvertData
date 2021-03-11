import logging
import azure.functions as func
import os


import logging
from .process_single_txt import *
from .send_queue_message import send_message


def main(msg: func.QueueMessage, output: func.Out[func.InputStream]):
    message = msg.get_body().decode('utf-8')

    connection_string = os.getenv("AzureWebJobsStorage")
    containerName = "raw-data"

    txt_to_csv = ['MB51', 'MB52', 'ZMB25', 'ZMM001', "MB51-MEP"]
    join_list_xlsx = ['ZMRP', 'MCBA']
    xlsx_to_csv = ['ZFI']

    if message == "all":
        for new_msg in txt_to_csv+join_list_xlsx+xlsx_to_csv:
            send_message(new_msg, connection_string, "uploadedfiles")
    elif message in txt_to_csv:
        outcsv = process_txtfile(message, connection_string, containerName)
        output.set(outcsv)
        logging.info(f"Success creating {message}.csv!")
    elif message in xlsx_to_csv:
        outcsv = process_xlsxfile(message, connection_string, containerName)
        output.set(outcsv)
        logging.info(f"Success creating {message}.csv!")
    elif message in join_list_xlsx:
        outcsv = join_xlsx(message, connection_string, containerName, subdir=f"{message}/")
        output.set(outcsv)
        logging.info(f"Success creating {message}.csv!")
    else:
        logging.info(f"No command defined for the message: {message}")


