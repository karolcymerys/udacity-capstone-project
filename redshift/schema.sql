CREATE EXTERNAL SCHEMA s3_schema
FROM DATA CATALOG
DATABASE 'udacity-capstone-project'
iam_role 'arn:aws:iam::$AWS_ACCOUNT_ID:role/$ENVIRONMENT_NAME-redshift-role'
CREATE EXTERNAL database IF NOT EXISTS;

CREATE TABLE dimTime (
    unique_id  BIGINT    IDENTITY(0,1),
    date_id    DATE      NOT NULL PRIMARY KEY SORTKEY,
    day        SMALLINT  NOT NULL,
    week       SMALLINT  NOT NULL,
    month      SMALLINT  NOT NULL,
    year       SMALLINT  NOT NULL,
    weekday    SMALLINT  NOT NULL
);

CREATE TABLE dimRegion (
    unique_id    BIGINT   IDENTITY(0,1),
    region_id    VARCHAR   NOT NULL PRIMARY KEY SORTKEY,
    super_region VARCHAR,
    name         VARCHAR   NOT NULL,
    country      VARCHAR   NOT NULL
);

CREATE TABLE factNewCase (
    date_id    DATE     NOT NULL PRIMARY KEY SORTKEY,
    region_id  VARCHAR  NOT NULL DISTKEY,
    new_cases  INT  NOT NULL,
    FOREIGN KEY (date_id)   REFERENCES dimTime(date_id),
    FOREIGN KEY (region_id) REFERENCES dimRegion(region_id)
);
