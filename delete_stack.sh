#!/bin/bash

ENVIRONMENT_NAME=dev
STACK=${ENVIRONMENT_NAME}-capstone-project

sam delete \
  --stack-name $STACK
