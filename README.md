# Data Convert - Python Azure Function

> This repository is part of the Construction Supply Chain Pipeline for the MSc Thesis developed in Mota-Engil with support from Faculty of Sciences of the University of Lisbon. 

The `Data Convert` function has the objective of processing specific files for a thesis project. The function is to be published in the Azure FunctionApp service. Conversion is currently being used for the following transactions:
* `"MB51"`
* `"MB52"`
* `"ZMB25"`
* `"MCBA"`
* `"ZMM001"`
* `"ZFI"`
* `"ZMRP"`
* `"MONOS"`
* `"PICPS"`
* `"SP99"`

**Note**: specifications about each of these files are to be given to the Mota-Engil group.

Some files are in TXT format while others are in XLSX in a single or multiple files. This function takes in account all these cases and processes them accordingly.

## How it works

A message must be sent to a queue storage (url of the queue storage has the format `https://{STORAGEACCOUNT}.queue.core.windows.net/{QUEUE_NAME}/`). The message must contain the name of the file it is intended to be processed and encoded in base64. The files to be processed must exist in `https://{STORAGEACCOUNT}.blob.core.windows.net/{CONTAINER}/`. To process all of the files without sending multiple messages, it can be sent a new message to the queue with the body: `"{CONTAINER}"`.

Currently we divide folders into deployment and maintenance. So in order to do this processing, it could be done the following way:

```bash
message=$(echo -n "deployment" | base64)

az storage message put \
  --content $message \
  -q $queueName \
  --time-to-live 120 \
  --auth-mode login \
  --account-name $storageAccount
```

The processed files will be posted in `{STORAGEACCOUNT}.blob.core.windows.net/data-ready/`.

## Code

The directory is organized in the following way:
 
    .
    ├── DataConvert/
    │   │
    │   ├── __init__.py     # Initializes and executes the pipeline
    │   │
    │   ├── process.py   # Contains the pipeline
    │   │
    │   ├── transform.py    # Single transaction functions
    │   │
    │   ├── function.json    # Configuration file for function in and output
    │   │
    │   └── settings.py   # Definitions for the processing
    │   
    ├── host.json   # Configuration host file 
    |
    ├── deployment.sh   # Initial bash deployment script
    |
    ├── maintenance.sh  # Script for when data is to be added       
    |
    └── requirements.txt  # Requirements for python code

The host.json is automatically created with the creation of the functionApp. But we still had to define the *function.json* according to what was intened. In this case we set the function app to initialize with a queue message. We use the already existing connection string to the storage to connect to the queue storage.


```json
{
  "scriptFile": "__init__.py",
  "bindings": [
    {
      "name": "msg",
      "type": "queueTrigger",
      "direction": "in",
      "queueName": "uploadedfiles",
      "connection": "AzureWebJobsStorage"
    },
  ]
}
```
*funtion.json*  

Inside the *\_\_init\_\_.py* file we have the function that will initialize upon the trigger given to the Azure Function. In this case, when the message in sent, the function initializes and will begin the processing of the container that the message indicates.

When we have the paths to all the files we want to process, we can initialize the ProcessingPipeline to transform all of the data into the format we want.

For example, in the case we have text files, we will use the `_process_text_files` method to process the file initially:

```python
    def _process_text_files(self):
        """Method that allows the processing of multiple text files"""
        logging.info(f"Processing {self.transaction} as TXT files.")
(...)
```
*process.py* 

The whole text file processing is fully automatized, but it should still be monitored for any changes.

For all the files, we should take in mind that they are expected to have the same structure, else, the dataframe concatenation will fail. The final CSV file will be named after their directory. For example the file in  `mb51/2020-01-01_mb51.TXT` will go to the file `mb51.csv` in data-ready container.

After having an initial version of a pandas DataFrame, we do all the transformation needed before uploading a cleaned version of the data inside the Azure SQL Database. The individual functions have the following format

```python
def prepare_mcba(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df['Matl type']=='ZMAT') | (df['Matl type']=='ZPEC')]
    (...)
```
*transform.py* 

All the different files have different transformations, therefore it was needed to build different functions for each of them. This should happen again for any added data file. This also makes any addition of transformation to be easier to do.

All these functions are processed within the `ProcessingPipeline`, turned into an output ready to be PUT back again into the Azure Data Lake Storage.This is written in bytes, and we explicitly write it in UTF-8 encoding.

## Learn more

Microsoft documentation mostly.

- [Information to edit the host json file](https://docs.microsoft.com/en-us/azure/azure-functions/functions-host-json "host.json reference for Azure Functions 2.x and later")
- [Information for trigger and binding](https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-blob-output?tabs=python "Azure Blob storage output binding for Azure Functions")
- [Information on creation of functions](https://docs.microsoft.com/en-us/azure/azure-functions/functions-reference-python "Azure Functions Python developer guide")
- [Information on using Azure-Storage-Blob](https://docs.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme?view=azure-python "Azure Storage Blobs client library for Python - Version 12.8.0")
- [Information on using Azure-Storage-Queue](https://docs.microsoft.com/en-us/python/api/overview/azure/storage-queue-readme?view=azure-python "Azure Storage Queues client library for Python - Version 12.1.5")