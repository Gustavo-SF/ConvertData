#!/bin/bash

export $(xargs < ../ppp.env)

message="deployment"

az storage message put \
  --content $(echo -n ${message} | base64) \
  -q $QUEUE_NAME \
  --time-to-live 120 \
  --auth-mode login \
  --account-name $STORAGE_ACCOUNT