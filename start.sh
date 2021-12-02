#!/bin/bash

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

export REDSHIFT_HOST=$(aws redshift describe-clusters --cluster-identifier ${ENVIRONMENT_NAME}-redshift-cluster | grep .redshift.amazonaws.com | cut -d "\"" -f4)
export TOPIC_ARN=$(aws sns list-topics | grep ${ENVIRONMENT_NAME}-sns-topic | cut -d "\"" -f4)

cd ./airflow

echo AIRFLOW_UID=1000 >> .env
echo AIRFLOW_CONN_AWS_CREDENTIALS=aws://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY >> .env
echo AIRFLOW_CONN_REDSHIFT_CONNECTION=postgresql://admin:AwsSecret123@$REDSHIFT_HOST:5439/udacity-capstone-project >> .env
echo AWS_REGION=$AWS_REGION >> .env
echo ENVIRONMENT_NAME=ENVIRONMENT_NAME >> .env
echo TOPIC_ARN=$TOPIC_ARN >> .env

docker-compose up
