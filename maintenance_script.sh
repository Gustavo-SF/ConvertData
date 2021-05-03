#!/bin/bash

functionappName="ConvertDataApp"

func azure functionapp publish $functionappName

# ensure the function has propagated
sleep 5s

is=("MB52" "MB51" "MCBA" "ZMRP" "ZFI" "ZMB25")
for ((i = 0; i < 6; i++)) 
	do az storage message put \
        --content $(echo -n ${is[i]} | base64) \
        -q $queueName \
        --time-to-live 120 \
        --auth-mode login \
        --account-name $storageAccount
done