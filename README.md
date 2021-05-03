# Data Convert - Python Azure Function

> This repository is part of the Construction Supply Chain Pipeline for the MSc Thesis developed in Mota-Engil with support from Faculty of Sciences of the University of Lisbon. 

The `Data Convert` function has the objective of processing specific files for a thesis project. The function is to be published in the Azure FunctionApp service. Conversion is meant to be used for the following files:
* `"MB51"`
* `"MB51-MEP"`
* `"MB52"`
* `"ZMB25"`
* `"MCBA"`
* `"ZMM001"`
* `"ZFI"`
* `"ZMRP"`
* `"ZMM001-Extra"`

**Note**: specifications about each of these files are to be given to the Mota-Engil group.

Some files are in TXT format while others are in XLSX in a single or multiple files. This function takes in account all these cases and processes them accordingly.

## How it works

A message must be sent to a queue storage (url of the queue storage has the format `https://{STORAGEACCOUNT}.queue.core.windows.net/uploadedfiles/`). The message must contain the name of the file it is intended to be processed and encoded in base64. The files to be processed must exist in `https://{STORAGEACCOUNT}.blob.core.windows.net/raw-data/`. To process all of the files without sending multiple messages, it can be sent a new message to the queue with the body: `"all"`. 

One way to do this processing could be in the following way:

```bash
message=$(echo -n "MESSAGE" | base64)

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
    │   ├── __init__.py     # Directs the message to the processing script
    │   │
    │   ├── process_single_txt.py   # Loads data and converts to dataframe
    │   │
    │   ├── singlefile_processing.py    # Individual file processing
    │   │
    │   ├── function.json    # Configuration file for function in and output
    │   │
    │   └── send_queue_message.py   # Function to send message to queue
    │   
    ├── host.json   # Configuration host file 
    |
    ├── deploy_script.sh   # Initial bash deployment script
    |
    ├── maintenance_script.sh  # Script for when data is to be added       
    |
    └── requirements.txt  # Requirements for python code

The host.json is automatically created with the creation of the functionApp. But we still had to define the *function.json* according to what was intened. In this case we set the function app to initialize with a queue message and to output a blob to a storage. Both for the queue and for the blob storage, we use the same ADLS to utilize the same access key.


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
    {
      "name": "output",
      "type": "blob",
      "path": "data-ready/{queueTrigger}.csv",
      "connection": "AzureWebJobsStorage",
      "direction": "out"
    }
  ]
}
```
*funtion.json*  

Insice the *\_\_init\_\_.py* file we have the function that will initialize upon the trigger given to the Azure Function. In this case, when the message in sent, the function initializes and will begin the processing of the file according to the message it was given. In this case the message contains the name of the file so it will use the message sent to the queue storage to build the file path and retrieve the wanted file.

There are three ways to process files in this function:

One is processing text files:
```python
def process_txtfile(msg, conn_string, container): 

    logging.info(f"Starting to process file {msg}...")

    blob = BlobClient.from_connection_string(conn_str=conn_string, container_name=container, blob_name=f"{msg}.TXT")
    blobStream = blob.download_blob().content_as_bytes()

    starts = {
        "MB52": 1,
        "MB51": 22,
        "ZMM001": 14,
        "ZMB25": 24,
        "MB51-MEP": 22,
        "ZMM001-Extra": 14,
    }
(...)
```
*process_single_txt.py* 

This is a very important part to consider since TXT files are constantly changing according to the needs of the user. The dictionary variable `starts` has a number associated with each of the TXT files. This number represents the first row with the headers for the final CSV file. In the case this number changes, the code must be deployed to Azure FunctionApp.

Another type of processing is done for XLSX files. When we have only one XLSX file the file is read from the blob file directly. In the case there are several files in a folder, we get all the files inside a folder, independent of the name, and we concatenate their contentes; this means they should have the same structure.

After having an initial version of a pandas DataFrame, we do all the transformation needed before uploading a cleaned version of the data inside the Azure SQL Database. 

```python
def prepare_mcba(df):
    logging.info("Starting to process MCBA")
    df = df[(df['Matl type']=='ZMAT') | (df['Matl type']=='ZPEC')]
    (...)
```
*singlefile_processing.py* 

All the different files have different transformations, therefore it was needed to build different functions for each of them. This should happen again for any added data file.

Finally, the DataFrame is returned to the function within the *process_single_txt.py*, turned into an output ready to be given as the output for the FunctionApp.This is written in bytes, and we explicitly write it in UTF-8 encoding.

The *send_queue_message.py* has the function that resends a queue message back to the queue storage. We only use this when we want all of the files to be processed (except for the ZMM001-Extra.TXT file), and this is done \_\_init\_\_.py.

## Learn more

Microsoft documentation mostly.

- [Information to edit the host json file](https://docs.microsoft.com/en-us/azure/azure-functions/functions-host-json "host.json reference for Azure Functions 2.x and later")
- [Information for trigger and binding](https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-blob-output?tabs=python "Azure Blob storage output binding for Azure Functions")
- [Information on creation of functions](https://docs.microsoft.com/en-us/azure/azure-functions/functions-reference-python "Azure Functions Python developer guide")
- [Information on using Azure-Storage-Blob](https://docs.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme?view=azure-python "Azure Storage Blobs client library for Python - Version 12.8.0")
- [Information on using Azure-Storage-Queue](https://docs.microsoft.com/en-us/python/api/overview/azure/storage-queue-readme?view=azure-python "Azure Storage Queues client library for Python - Version 12.1.5")