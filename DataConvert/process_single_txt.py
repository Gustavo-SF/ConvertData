import logging
import pandas as pd
from azure.storage.blob import BlobClient, ContainerClient
import io
      
      
def process_txtfile(msg, conn_string, container): 

    logging.info(f"Starting to process file {msg}...")

    blob = BlobClient.from_connection_string(conn_str=conn_string, container_name=container, blob_name=f"{msg}.TXT")
    blobStream = blob.download_blob().content_as_bytes()

    starts = {
        "MB52": 1,
        "MB51": 21,
        "ZMM001": 14,
        "ZMB25": 24,
        "MB51-MEP": 26,
    }

    df_list = []
    with io.BytesIO(blobStream) as txt:
        for i, row in enumerate(txt):
            row = row.decode('iso-8859-1')
            if i == starts[msg]:
                col_len = len(row)
                cols = row.split("|")
                cols = [a.strip() for a in cols[1:-1]]
                split_indices = [i for i, ltr in enumerate(row) if ltr=='|']
            if (i > starts[msg]+1):
                if len(row) == col_len:
                    if  row[split_indices[-2]]=='|':
                        list_to_append = [row[i+1:j].strip() for i,j in zip(split_indices[:-1], split_indices[1:None])]
                        # logging.info('The Row:')
                        # logging.info(row)
                        # logging.info("List to Append:")
                        # logging.info(list_to_append)
                        df_list.append((list_to_append))

    df = pd.DataFrame(df_list)
    df.columns = cols

    # Remove the repeated rows with the column names
    df = df[df[df.columns[1]]!=df.columns[1]]

    # output
    outcsv = df.to_csv(index=False)
    
    return outcsv


def process_xlsxfile(msg, conn_string, container):
    logging.info(f"Starting to process file {msg}...")
    blob = BlobClient.from_connection_string(conn_str=conn_string, container_name=container, blob_name=f"{msg}.XLSX")
    df = pd.read_excel(blob.download_blob().content_as_bytes())
    
    outcsv = df.to_csv(index=False)

    return outcsv


def join_xlsx(msg, conn_string, container, subdir):
    logging.info(f"Starting to process folder {msg}...")

    blobs_ls = ContainerClient.from_connection_string(conn_str=conn_string, container_name=container).list_blobs(name_starts_with=subdir)

    final_df = pd.DataFrame()

    for blob in blobs_ls:
        blob = BlobClient.from_connection_string(conn_str=conn_string, container_name=container, blob_name=blob.name)
        df = pd.read_excel(blob.download_blob().content_as_bytes())
        final_df = pd.concat([final_df, df])
    
    outcsv = final_df.to_csv(index=False)

    return outcsv
