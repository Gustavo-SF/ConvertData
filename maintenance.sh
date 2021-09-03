#!/bin/bash
# This script will upload the new data placed in maintenance
# upload it, and process it with the function app.

# We need a virtual environment with requirements installed
source .venv/bin/activate
# Before uploading we need the right connection string
con_string=$(az storage account show-connection-string -n $STORAGE_ACCOUNT -o tsv)
# prepare con_string code to be read by sed function
con_string=$(echo $con_string | sed 's/&/\\\&/g;s/\//\\\//g')

# We put the secret connection string into settings
sed "s/CONNECTION_STRING/${con_string}/" local.settings.json -i

python move_files_to_storage.py maintenance

echo "[PPP] Upload terminated for storage account"

echo "[PPP] Starting maintenance transformations"

# post message
body=`echo -n "maintenance" | base64`

az storage message put \
    --content $body \
    -q $QUEUE_NAME \
    --time-to-live 120 \
    --auth-mode login \
    --account-name $STORAGE_ACCOUNT \
    --output none

# Requests to storage take a lot of time
sleep 10m