#!/bin/bash

cd ./airflow
rm ./.env
docker-compose down -v
cd ..

aws s3 rm s3://${ENVIRONMENT_NAME}-udacity-capstone-project/raw_data --recursive

STACK=${ENVIRONMENT_NAME}-capstone-project

sam delete \
  --stack-name $STACK
