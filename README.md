# Data Convert - Python Azure Function

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

A message must be sent to a queue storage (with the format `https://{STORAGEACCOUNT}.queue.core.windows.net/uploadedfiles/`). The message must contain the name of the file it is intended to be processed encoded in base64. The files to be processed must exist in `https://{STORAGEACCOUNT}.blob.core.windows.net/raw-data/`. To process all of the files without sending multiple messages, it can be sent a new message to the queue with the body: `"all"`. 

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
    └── host.json   # Configuration host file 

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
## Learn more

Microsoft documentation mostly.

- [Information to edit the host json file](https://docs.microsoft.com/en-us/azure/azure-functions/functions-host-json "host.json reference for Azure Functions 2.x and later")
- [Information for trigger and binding](https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-blob-output?tabs=python "Azure Blob storage output binding for Azure Functions")
- [Information on creation of functions](https://docs.microsoft.com/en-us/azure/azure-functions/functions-reference-python "Azure Functions Python developer guide")
- [Information on using Azure-Storage-Blob](https://docs.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme?view=azure-python "Azure Storage Blobs client library for Python - Version 12.8.0")
- [Information on using Azure-Storage-Queue](https://docs.microsoft.com/en-us/python/api/overview/azure/storage-queue-readme?view=azure-python "Azure Storage Queues client library for Python - Version 12.1.5")