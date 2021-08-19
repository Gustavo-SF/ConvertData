#!/bin/bash
# This script will upload the new data placed in maintenance
# upload it, and process it with the function app.

# We need a virtual environment with requirements installed
source .venv/bin/activate
python move_files_to_storage.py maintenance

echo "[PPP] Upload terminated for storage account"

func azure functionapp publish $FUNCTIONAPP_NAME

# ensure the function has propagated
sleep 5s

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
sleep 60m