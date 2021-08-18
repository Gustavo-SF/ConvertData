#!/bin/bash

functionappName="ConvertDataApp"

func azure functionapp publish $functionappName

# ensure the function has propagated
sleep 5s

queueName="uploadedfiles"
storageAccount="dsthesissa"

for transaction in "MB52" "MB51" "MCBA" "ZFI" "ZMB25"
	do az storage message put \
        --content $(echo -n ${transaction} | base64) \
        -q $queueName \
        --time-to-live 120 \
        --auth-mode login \
        --account-name $storageAccount
done