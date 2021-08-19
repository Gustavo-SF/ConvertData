#!/bin/bash
# Deployment script for DataConvert Queue Trigger function.
# 
# It allows to process all of the data before uploading into
# Azure Data Lake Storage Account

az functionapp create \
    --consumption-plan-location $LOCATION \
    --runtime python \
    --runtime-version 3.8 \
    --functions-version 3 \
    --name $FUNCTIONAPP_NAME \
    --storage-account $STORAGE_ACCOUNT \
    --os-type linux \
    --disable-app-insights true \
    --output none


echo "[PPP] Uploading existing files into the storage account"

# Before uploading we need the right connection string
con_string=$(az storage account show-connection-string -n $STORAGE_ACCOUNT -o tsv)
# prepare con_string code to be read by sed function
con_string=$(echo $con_string | sed 's/&/\\\&/g;s/\//\\\//g')

# We put the secret connection string into settings
sed "s/CONNECTION_STRING/${con_string}/" local.settings.json -i

# We need a virtual environment with requirements installed
source .venv/bin/activate
python move_files_to_storage.py deployment

echo "[PPP] Upload terminated for storage account"

func azure functionapp publish $FUNCTIONAPP_NAME

# ensure the function has propagated
sleep 5s

# post message
body=`echo -n "deployment" | base64`

az storage message put \
    --content $body \
    -q $QUEUE_NAME \
    --time-to-live 120 \
    --auth-mode login \
    --account-name $STORAGE_ACCOUNT \
    --output none

# Requests to storage take a lot of time
sleep 60m