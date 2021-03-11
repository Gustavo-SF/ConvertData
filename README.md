# Data Convert - Python

The `Data Convert` function has the objective of converting specific files for a thesis project. The function is to be published in the Azure FunctionApp service. Conversion here include for the following files:
* MB51
* MB51-MEP
* MB52
* ZMB25
* MCBA
* ZMM001
* ZFI
* ZMRP

Some files come in a text file and others are in excel in a single or seperated files. This function takes in account all these cases and processes accordingly.

## How it works

A queue message must be left on https://dsthesissa.queue.core.windows.net/uploadedfiles with the name of the file it is intended to be processed. To process all a queue message can be dropped with the body: `all`

## Learn more

Microsoft documentation mostly.
[Information to edit the host json file](https://docs.microsoft.com/en-us/azure/azure-functions/functions-host-json "host.json reference for Azure Functions 2.x and later")
[Information for trigger and binding](https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-blob-output?tabs=python "Azure Blob storage output binding for Azure Functions")
[Information on creation of functions](https://docs.microsoft.com/en-us/azure/azure-functions/functions-reference-python "Azure Functions Python developer guide")
[Information on using Azure-Storage-Blob](https://docs.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme?view=azure-python "Azure Storage Blobs client library for Python - Version 12.8.0")
[Information on using Azure-Storage-Queue](https://docs.microsoft.com/en-us/python/api/overview/azure/storage-queue-readme?view=azure-python "Azure Storage Queues client library for Python - Version 12.1.5")