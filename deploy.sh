#!/bin/bash

ENVIRONMENT_NAME=dev
S3_BUCKET=cloudformation-deploy-s3-bucket
S3_PREFIX=templates/${ENVIRONMENT_NAME}
OUTPUT_TEMPLATE=$(mktemp /tmp/tempfileXXXXXX)
STACK=${ENVIRONMENT_NAME}-capstone-project
LOGGING_LEVEL=INFO

sam build --use-container \
  --template-file infrastructure.yaml \
  --cached

sam package \
  --s3-bucket ${S3_BUCKET} \
  --s3-prefix ${S3_PREFIX} \
  --output-template-file ${OUTPUT_TEMPLATE}

sam deploy \
  --template-file ${OUTPUT_TEMPLATE} \
  --stack-name ${STACK} \
  --s3-bucket ${S3_BUCKET} \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --confirm-changeset \
  --parameter-overrides \
  EnvironmentName=${ENVIRONMENT_NAME} \
  LoggingLevel=${LOGGING_LEVEL}
