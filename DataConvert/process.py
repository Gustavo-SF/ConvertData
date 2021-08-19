"""
The Pipeline to use in the main function is here.
It should hardly require any maintenance. 

It is already prepared to transform what we define as type (TXT, XLSX and CSV),
and download and upload into Blob Storage. In case we want to upload locally,
we can also set UPLOAD_LOCALLY to True.
"""
import logging
import io
from pathlib import Path
from statistics import mode
from typing import List

import pandas as pd

from .settings import files_definitions


UPLOAD_LOCALLY = False


class ProcessingPipeline:
    """Main processing pipeline"""
    def __init__(self, transaction: str, files: List[str], blob_client, blob_container_client):
        self.transaction = transaction
        self.files = files
        self.blob_client = blob_client
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
            raise NotImplementedError(f"Type of file processing for {type_of_file} has not been implemented yet!")

    def upload(self):
        outcsv = self.df.to_csv(index=False, encoding='utf-8')

        if UPLOAD_LOCALLY:
            data_ready_dir = Path(__file__).parent.parent / "data" / "data-ready"
            data_ready_dir.mkdir(exist_ok=True)
            with open(data_ready_dir / f"{self.transaction}.csv", "w") as f:
                f.write(outcsv)
        
        else:
            blob = self.blob_client.get_blob_client(container="data-ready", blob=f"{self.transaction}.csv")
            blob.upload_blob(outcsv)

    def _process_text_files(self):
        """Method that allows the processing of multiple text files"""
        logging.info(f"Processing {self.transaction} as TXT files.")

        # Getting the function
        function_for_file = files_definitions[self.transaction]["function"]

        dataframe = pd.DataFrame()

        for file in self.files:
            blob_content = self.blob_container_client.get_blob_client(blob=file).download_blob().content_as_text(encoding="cp1252")

            # We search for the first line with the most "|" in them. This should be the header for the TXT file
            value_to_search = mode([len(line.split("|")) for line in blob_content.split("\n")])
            
            line_ln = mode([len(line) for line in blob_content.split("\n")])

            for i, line in enumerate(blob_content.split("\n")):
                if (len(line.split("|"))==value_to_search) & (len(line) == line_ln):
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
            df = self._simplify_columns(df)
            df = function_for_file(df)

            dataframe = pd.concat([dataframe, df])
    
        return dataframe

    def _process_xlsx_files(self):
        """Method that allows the processing XLSX files"""
        logging.info(f"Processing {self.transaction} as XLSX files.")

        # Getting the function
        function_for_file = files_definitions[self.transaction]["function"]

        dataframe = pd.DataFrame()

        for file in self.files:
            blob_content = self.blob_container_client.get_blob_client(blob=file).download_blob().content_as_bytes()
            df = pd.read_excel(blob_content, dtype=str)
            df = self._simplify_columns(df)
            df = function_for_file(df)

            dataframe = pd.concat([dataframe, df])
    
        return dataframe

    def _process_csv_files(self):
        """Method that allows the processing of CSV files"""
        logging.info(f"Processing {self.transaction} as CSV files.")

        # Getting the function
        function_for_file = files_definitions[self.transaction]["function"]

        dataframe = pd.DataFrame()

        for file in self.files:
            blob_content = self.blob_container_client.get_blob_client(blob=file).download_blob().content_as_bytes()
            df = pd.read_csv(io.BytesIO(blob_content), encoding="cp1252", dtype=str)
            df = self._simplify_columns(df)
            df = function_for_file(df)

            dataframe = pd.concat([dataframe, df])
    
        return dataframe

    @staticmethod
    def _simplify_columns(df: pd.DataFrame) -> pd.DataFrame:
        # Columns uniformization
        df.columns = [col.lower().strip() for col in df.columns]
        return df

        


