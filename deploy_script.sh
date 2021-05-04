#!/bin/bash

functionappName="ConvertDataApp"

az functionapp create \
    --consumption-plan-location $location \
    --runtime python \
    --runtime-version 3.8 \
    --functions-version 3 \
    --name $functionappName \
    --storage-account $storageAccount \
    --os-type linux

func azure functionapp publish $functionappName

# ensure the function has propagated
sleep 5s

# post message
body=`echo -n 'all' | base64`

az storage message put \
    --content $body \
    -q $queueName \
    --time-to-live 120 \
    --auth-mode login \
    --account-name $storageAccount

# MCBA XLSX files take a lot of time to process
sleep 50m