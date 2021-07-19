import logging
import io

import pandas as pd
from azure.storage.blob import BlobClient, ContainerClient

from .settings import starts, functions, LogMessages as LOGS
      
      
def process_text_file(msg: str, conn_string: str, container: str) -> str: 
    """Function to process text data and create a dataframe"""

    logging.info(LOGS.process_text.format(msg=msg))

    # Get data from blob storage
    blob = BlobClient.from_connection_string(conn_str=conn_string, container_name=container, blob_name=f"{msg}.TXT")
    blobStream = blob.download_blob().content_as_bytes()

    # Process text file
    df_list = []
    with io.BytesIO(blobStream) as txt:
        for i, row in enumerate(txt):
            row = row.decode('cp1252') #ISO-8859-1 was here before 
            if (i > max(1, starts[msg])) and (i < starts[msg]+3):
                logging.info(row)
            if i == starts[msg]:
                col_len = len(row)
                cols = row.split("|")
                cols = [a.strip() for a in cols[1:-1]]
                split_indices = [i for i, ltr in enumerate(row) if ltr=='|']
                logging.info("These are the indices taken from the above columns:")
                logging.info(split_indices)
            if (i > starts[msg]+1):
                if len(row) == col_len:
                    if  row[split_indices[-2]]=='|':
                        list_to_append = [row[i+1:j].strip() for i,j in zip(split_indices[:-1], split_indices[1:None])]
                        df_list.append((list_to_append))


    df = pd.DataFrame(df_list)
    df.columns = cols

    # Remove the repeated rows with the column names
    df = df[df[df.columns[1]]!=df.columns[1]]
    df = functions[msg](df)

    # Output
    outcsv = df.to_csv(index=False, encoding='utf-8')
    
    return outcsv


def process_xlsx_file(msg: str, conn_string: str, container: str) -> str:
    """Function to process excel data and create a dataframe"""

    logging.info(LOGS.process_xlsx.format(msg=msg))

    # Get data from blob storage
    blob = BlobClient.from_connection_string(conn_str=conn_string, container_name=container, blob_name=f"{msg}.XLSX")
    df = pd.read_excel(blob.download_blob().content_as_bytes())

    df = functions[msg](df)
    
    outcsv = df.to_csv(index=False, encoding='utf-8')

    return outcsv


def process_xlsx_folder(msg: str, conn_string: str, container: str, subdir: str) -> str:
    """Function to process excel folder data and create a dataframe"""

    logging.info(LOGS.process_xlsx_folder.format(msg=msg))

    # Get data from blob storage
    blobs_ls = ContainerClient.from_connection_string(conn_str=conn_string, container_name=container).list_blobs(name_starts_with=subdir)
    final_df = pd.DataFrame()
    for i, blob in enumerate(blobs_ls):
        blob = BlobClient.from_connection_string(conn_str=conn_string, container_name=container, blob_name=blob.name)
        df = pd.read_excel(blob.download_blob().content_as_bytes())
        if i==0:
            cols = df.columns
        else:
            df.columns = cols
        final_df = pd.concat([final_df, df])
    
    final_df = functions[msg](final_df)

    outcsv = final_df.to_csv(index=False, encoding='utf-8')

    return outcsv