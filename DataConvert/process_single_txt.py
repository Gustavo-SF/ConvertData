import logging
import pandas as pd
from azure.storage.blob import BlobClient, ContainerClient
import io
from .singlefile_processing import *
      
      
def process_txtfile(msg, conn_string, container): 

    logging.info(f"Starting to process file {msg}...")

    blob = BlobClient.from_connection_string(conn_str=conn_string, container_name=container, blob_name=f"{msg}.TXT")
    blobStream = blob.download_blob().content_as_bytes()

    starts = {
        "MB52": 1,
        "MB51": 21,
        "ZMM001": 14,
        "ZMB25": 24,
        "MB51-MEP": 21,
        "ZMM001-Extra": 14,
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
                        df_list.append((list_to_append))

    df = pd.DataFrame(df_list)
    df.columns = cols

    # Remove the repeated rows with the column names
    df = df[df[df.columns[1]]!=df.columns[1]]

    if msg=='MB52':
        df = prepare_mb52(df)
    elif msg=='MB51':
        df = prepare_mb51(df)
    elif msg=='MB51-MEP':
        df = prepare_mb51mep(df)
    elif msg=='ZMM001' or msg=='ZMM001-Extra':
        df = prepare_zmm001(df)
    elif msg=='ZMB25':
        df = prepare_zmb25(df)
    # output
    outcsv = df.to_csv(index=False)
    
    return outcsv


def process_xlsxfile(msg, conn_string, container):
    logging.info(f"Starting to process file {msg}...")
    blob = BlobClient.from_connection_string(conn_str=conn_string, container_name=container, blob_name=f"{msg}.XLSX")
    df = pd.read_excel(blob.download_blob().content_as_bytes())

    if msg=='ZFI':
        df = prepare_zfi(df)
    
    outcsv = df.to_csv(index=False)

    return outcsv


def join_xlsx(msg, conn_string, container, subdir):
    logging.info(f"Starting to process folder {msg}...")

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
    
    if msg=='MCBA':
        final_df = prepare_mcba(final_df)
    elif msg=='ZMRP':
        final_df = prepare_mrp(final_df)

    outcsv = final_df.to_csv(index=False)
    return outcsv