import logging
import io
from pathlib import Path
from statistics import mode
from azure.storage import blob

import pandas as pd
from azure.storage.blob import BlobServiceClient

from .settings import LogMessages as LOGS, files_definitions


UPLOAD_LOCALLY = True
      

class ProcessingPipeline:
    def __init__(self, transaction, files, blob_container_client):
        self.transaction = transaction
        self.files = files
        self.blob_container_client = blob_container_client

    def transform(self):
        type_of_file = files_definitions[self.transaction]["type"]

        if type_of_file == "txt":
            self.df = self._process_text_files()
        elif type_of_file == "xlsx":
            self.df = self._process_xlsx_files()
        elif type_of_file == "csv":
            self.df = self._process_csv_files()
        else:
            raise Exception(f"Type of file processing for {type_of_file} has not been implemented yet!")

    def upload(self):
        outcsv = self.df.to_csv(index=False, encoding='utf-8')

        if UPLOAD_LOCALLY:
            data_ready_dir = Path(__file__).parent.parent / "data-ready"
            data_ready_dir.mkdir(exist_ok=True)
            with open(data_ready_dir / f"{self.transaction}.csv", "w") as f:
                f.write(outcsv)
        
        else:
            pass

    def _process_text_files(self):
        """Method that allows the processing of multiple text files"""
        logging.info("Processing text files")

        # Getting the function
        function_for_file = files_definitions[self.transaction]["function"]

        dataframe = pd.DataFrame()

        for file in self.files:
            blob_content = self.blob_container_client.get_blob_client(blob=file).download_blob().content_as_text(encoding="cp1252")

            # We search for the first line with the most "|" in them. This should be the header for the TXT file
            value_to_search = mode([len(line.split("|")) for line in blob_content.split("\n")])
            for i, line in enumerate(blob_content.split("\n")):
                if len(line.split("|"))==value_to_search:
                    logging.info(line)
                    starts = i
                    break
            
            # Process text file
            df_list = []
            for i, row in enumerate(blob_content.split("\n")):
                if (i > max(1, starts)) and (i < starts+3):
                    logging.debug(row)
                if i == starts:
                    col_len = len(row)
                    cols = row.split("|")
                    cols = [a.strip() for a in cols[1:-1]]
                    split_indices = [i for i, ltr in enumerate(row) if ltr=='|']
                    logging.info("These are the indices taken from the above columns:")
                    logging.info(split_indices)
                if (i > starts+1):
                    if len(row) == col_len:
                        if  row[split_indices[-2]]=='|':
                            list_to_append = [row[i+1:j].strip() for i,j in zip(split_indices[:-1], split_indices[1:None])]
                            df_list.append((list_to_append))

            df = pd.DataFrame(df_list)
            df.columns = cols

            # Remove the repeated rows with the column names
            df = df[df[df.columns[1]]!=df.columns[1]]
            df = function_for_file(df)

        dataframe = pd.concat([dataframe, df])
    
        return dataframe


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
        df = pd.read_excel(blob.download_blob().content_as_bytes(), dtype=str)
        if i==0:
            cols = df.columns
        else:
            df.columns = cols
        final_df = pd.concat([final_df, df])
    
    final_df = functions[msg](final_df)

    outcsv = final_df.to_csv(index=False, encoding='utf-8')

    return outcsv