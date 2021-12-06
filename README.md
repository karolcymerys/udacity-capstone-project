# Udacity - Capstone Project

## Overall 
This is a final project for Udacity Course: Data Engineering Nanodegree Program. 
The main goal of this project is to prepare data pipeline that is extracting, transforming and loading data to database.
In this case, ETL Data Pipeline was prepared to import statistics related to COVID-19 infections to data warehouse, so they can be easily analyzed using SQL queries. 

### Project requirements
- COVID-19 statistics from at least two sources should be downloaded (using at least two data formats)
- Data downloading mechanism should be scalable (greatly increasing the amount of data to download shouldn't cause any problems)
- Downloaded data should be imported to staging tables in Amazon Redshift
- Data from staging tables should be extracted, transformed and loaded to multi-dimensional model
- There shouldn't be any duplicates in dimension tables
- Raw data should be removed once the ETL Data Pipeline processing is completed

### Technology Stack
- Amazon Redshift
- Apache Airflow
- AWS Cloud Formation
- AWS Lambda
- AWS Simple Storage Service
- AWS SNS
- AWS SQS
- Redshift Spectrum

### Multi-dimensional model
Data are loaded to following multi-dimensional model:
![Multi-dimensional model](docs/schema.png?raw=true "Multi-dimensional model")

This model allows to research data by filtering and aggregating data by date and region, where new infections occurred. 

### Data dictionary

| Column Name  | Description |
| ------------- | ------------- |
| unique_id  | Unique id in dimension table to recognized duplicates |
| new_cases  | Number of new infections in given day and region  |
| date_id  | Date id in format YYYY-MM-DD  |
| day  | Day of month  |
| month  | Month of year  |
| year  | Year  |
| weekday  | Day of week  |
| week  | Week of year  |
| region_id  | Content Cell  |
| name  | Region name  |
| super_region  | Name of super region  |
| country  | Country of region  |

### Data Sources
Prepared ETL Data Pipeline allows import data for two countries: United Kingdom and United States, thus two data sources are utilized:

1. [COVID-19 Statistics from United Kingdom](https://api.coronavirus.data.gov.uk):\
This RESTful Web Services provides endpoint that allows to get new COVID-19 infections for each day in JSON, CSV, XML (this project utilizes JSON format). 
Example response:
```
{
    "length": 380,
    "maxPageLimit": 2500,
    "totalRecords": 380,
    "data": [
        {
            "date": "2021-11-22",
            "area_name": "Hartlepool",
            "area_code": "E06000001",
            "cases": 53
        }
    ],
    ...
}
```
Above response returns data as a JSON array and contains following details:
- date
- region name (`area_name`)
- new infections (`cases`)  

2. [The New York Times. (2021). Coronavirus (Covid-19) Data in the United States](https://github.com/nytimes/covid-19-data)\
This GitHub repository contains multiple csv files that are updated every day. 
[One of them](https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv) allows to get COVID-19 statistics for each county.

The structure of this file is as follows:
- date,
- county,
- state,
- fips (unique identifier of region, however some records contains empty values),
- cases (total number of recorded cases for this county since the pandemic occurred),
- deaths (total number of recorded deaths for this county since the pandemic occurred).

## ETL Data Pipeline: Apache Airflow DAG

ETL Data Pipeline was prepared as Apache Airflow's DAG. Following diagram shows the whole flow of this ETL Data Pipeline:
![Apache Airflow DAG](docs/airflow.png?raw=true "Apache Airflow DAG")


### Stream Sources
First step of this ETL Data Pipeline is responsible for streaming URLs of data source to SNS Topic. 
Next, notifications are filtered and sent to proper SQS. Consumers of these queues (AWS Lambda) are downloading data from given source and saving them to S3 Bucket. 
This process is illustrated on following diagram: 

![Fetching data mechanism](docs/fetching_data.png?raw=true "Fetching data mechanism")

It is important to notice that this approach allows to customize Lambda consumers to given source. 
Also, maximum number of concurrent lambda invocation was introduced to not send to many request to the same source at once.

There was one assumption taken: ETL Data Pipeline can be executed only once in the same time and any other process cannot send messages to SNS nor SQS.

### Waiting for queues to be empty
Once all links are streamed to SNS Topic, DAG is going to the next step, where it waits until all SQS queues are empty (all data are downloaded). 
In order to do that, Apache Airflow's Sensor was utilized to check in every 30 seconds whether queues are empty.   

### Staging tables creation
In next step staging tables are created. In case of this ETL Data Pipeline raw data are not loaded directly to Redshift, but they are stored in S3 Bucket.
Redshift Spectrum allows to create external tables that will be a reference to files saved in S3 and can be queried by Redshift. 
Redshift Spectrum supports both formats, CSV and JSON.

### Data Quality Check
Once staging tables are created, data quality check is done. There are executed two queries:
- `SELECT count(*) FROM s3_schema.uk_source s WHERE s.date_time IS NULL OR area_code is NULL OR new_cases IS NULL`
- `SELECT count(*) FROM s3_schema.usa_source s WHERE s."date" IS NULL OR cases IS NULL`

Both queries are checking the most crucial fields in terms of assumed model. Result of both SQL queries should be equal to `0`.

### Loading data to Multi-dimensional model
When the Data Quality Check success, then data from staging tables are loaded to multi-dimensional model. 

### Handling duplicates
Even though SQL queries that loads data to dimension tables contains `DISTINCT` command, it doesn't prevent from having duplicates in dimensions tables. 
That's why this step is responsible for removing duplicates from dimension tables if any exist.

### Cleanup
Cleanup is a step that is not explicitly added as previous step, because it's invoked always when DAG's processing is finished (even if processing fails).
That approach allows to remove external staging tables and data from S3 Bucket.

## Answers

### The data was increased by 100x.
Prepared ETL Data Pipeline was designed to prevent crashing when data traffic will be increased. 
In order to prevent that data buffer (SNS Topic and SQS queues) was prepared. 
When incoming data will be increased by 100x, this mechanism will adjust data consumers to number of messages in SQS. 
When Lambda will reach maximum number of concurrent invocation, then some data will be queued, and they will wait for their turn.

### The pipelines would be run on a daily basis by 7 am every day.
This scenario makes sense to run ETL data pipeline to download daily statistics. 
Actually this mechanism is almost ready to cover that scenario. 
We would only need to use proper sources that would provide statistics with new cases per day (not total number of infections since pandemic occurred).

### The database needed to be accessed by 100+ people.
When data in database would be needed to be accessed by more than 100 people, then probably it would be needed to increase resources (CPU's, memory, number of nodes) for Redshift Cluster.

## Run ETL Project
### Requirements
- Unix OS
- AWS CLI
- AWS SAM
- Docker
- Docker compose

### Building infrastructure and running ETL Data Pipeline
In scope of this project AWS infrastructure was prepared as AWS CloudFormation templates, so whole infrastructure can be easily prepared.
Deployment script is located in `start.sh`.

Before starting, keep in mind that running that script might charge you from using AWS Services, so please use this script carefully. 
Author is not responsible for use of this code and the costs incurred.

1. Fill in environmental variables in `env_vars`:
```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_REGION=
export ENVIRONMENT_NAME=
```
Please keep in mind that `ENVIRONMENT_NAME` should be unique across all AWS environment and will be automatically transformed to lower cases.

2. Execute command `source env_vars`
3. Grant permission to execute script: 
```
chmod +x start.sh
chmod +x clean.sh
```
4. Run script `start.sh`. Make sure that you have access to build docker images as lambda will be built in docker containers. This script will build needed AWS infrastructure, create tables in Amazon Redshift and run Apache Airflow in Docker Environment. Connections will be added automatically to Apache Airflow as environmental variables.
5. Go to `localhost:8080`. User: `airflow`, password `airflow`
6. Run ETL Data Pipeline: `covid_19_statistics`
7. In order to analyze imported data execute command `aws redshift describe-clusters --cluster-identifier ${ENVIRONMENT_NAME}-redshift-cluster | grep .redshift.amazonaws.com | cut -d "\"" -f4`. It will return host of Redshift Cluster. Please use returned value to connect to Redshift Cluster using your favourite Database Client (Database: `udacity-capstone-project`, User: `admin`, password: `AwsSecret123`).
8. Hit `Ctrl + C`, when you finish working with this project
9. Run script `clean.sh` to remove AWS infrastructure

## Demo
### DAG is successfully complete
![DAG is successfully complete](docs/airflow-tree.png?raw=true "DAG is successfully complete")

### How many of infections were recorded in 11-th November 2011 in each country?
![How many of infections were recorded in 11-th November 2011 in each country?](docs/query_1.png?raw=true "How many of infections were recorded in 11-th November 2011 in each country?")

### On which day of the week the average number of new infections was the greatest? 
![On which day of the week the average number of new infections was the greatest?](docs/query_2.png?raw=true "On which day of the week the average number of new infections was the greatest?")

### What is the average number of new infections for next months for each country? 
![What is the average number of new infections for next months for each country?](docs/query_3.png?raw=true "What is the average number of new infections for next months for each country?")
