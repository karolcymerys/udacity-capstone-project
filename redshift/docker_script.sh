#!/bin/bash

envsubst < schema_template.sql > schema.sql
PGPASSWORD=AwsSecret123 psql -h $REDSHIFT_HOST -p 5439 -U admin -d udacity-capstone-project -a -f schema.sql
