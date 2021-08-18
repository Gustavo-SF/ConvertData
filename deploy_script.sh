#!/bin/bash

az functionapp create \
    --consumption-plan-location $LOCATION \
    --runtime python \
    --runtime-version 3.8 \
    --functions-version 3 \
    --name $FUNCTIONAPP_NAME \
    --storage-account $STORAGE_ACCOUNT \
    --os-type linux

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
    --account-name $STORAGE_ACCOUNT

# MCBA XLSX files take a lot of time to process
sleep 50m