CREATE EXTERNAL SCHEMA s3_schema
FROM DATA CATALOG
DATABASE 'udacity-capstone-project'
iam_role 'arn:aws:iam::534172043736:role/dev-redshift-role'
CREATE EXTERNAL database IF NOT EXISTS;

CREATE TABLE dimTime (
    date_id    BIGINT    NOT NULL PRIMARY KEY SORTKEY,
    day        SMALLINT  NOT NULL,
    week       SMALLINT  NOT NULL,
    month      SMALLINT  NOT NULL,
    year       SMALLINT  NOT NULL,
    weekday    SMALLINT  NOT NULL
)

CREATE TABLE dimArea (
    area_id    VARCHAR   NOT NULL PRIMARY KEY SORTKEY,
    city       VARCHAR   NOT NULL,
    country    VARCHAR   NOT NULL
)

CREATE EXTERNAL TABLE s3_schema.factCovid_stats(
	area_id   varchar,
	new_cases int)
PARTITIONED by (date_id bigint)
STORED as PARQUET
location 's3://dev-udacity-capstone-project/fact_tables/covid_stats/';
