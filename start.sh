#!/bin/bash

ENVIRONMENT_NAME=$(echo $ENVIRONMENT_NAME | tr '[:upper:]' '[:lower:]')
S3_BUCKET=cloudformation-deploy-s3-bucket
S3_PREFIX=templates/${ENVIRONMENT_NAME}
OUTPUT_TEMPLATE=$(mktemp /tmp/tempfileXXXXXX)
STACK=${ENVIRONMENT_NAME}-capstone-project
LOGGING_LEVEL=INFO

# Deploy AWS Resources
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

# Build Redshift schema
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity | grep Account | cut -d "\"" -f4)
export REDSHIFT_HOST=$(aws redshift describe-clusters --cluster-identifier ${ENVIRONMENT_NAME}-redshift-cluster | grep .redshift.amazonaws.com | cut -d "\"" -f4)
export TOPIC_ARN=$(aws sns list-topics | grep ${ENVIRONMENT_NAME}-sns-topic | cut -d "\"" -f4)

cd ./redshift
docker build -t debian-psql .
docker run \
-e AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID \
-e ENVIRONMENT_NAME=$ENVIRONMENT_NAME \
-e REDSHIFT_HOST=$REDSHIFT_HOST \
debian-psql


cd ../airflow

echo AIRFLOW_UID=1000 >> .env
echo AIRFLOW_CONN_AWS_CREDENTIALS=aws://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY@ >> .env
echo AIRFLOW_CONN_REDSHIFT_CONNECTION=postgresql://admin:AwsSecret123@$REDSHIFT_HOST:5439/udacity-capstone-project >> .env
echo AWS_REGION=$AWS_REGION >> .env
echo ENVIRONMENT_NAME=$ENVIRONMENT_NAME >> .env
echo TOPIC_ARN=$TOPIC_ARN >> .env

# Run Apache Airflow in docker containers
docker-compose up
