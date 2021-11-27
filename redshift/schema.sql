CREATE EXTERNAL SCHEMA s3_schema
FROM DATA CATALOG
DATABASE 'udacity-capstone-project'
iam_role 'arn:aws:iam::534172043736:role/dev-redshift-role'
CREATE EXTERNAL database IF NOT EXISTS;

CREATE TABLE dimTime (
    date_id    DATE      NOT NULL PRIMARY KEY SORTKEY,
    day        SMALLINT  NOT NULL,
    week       SMALLINT  NOT NULL,
    month      SMALLINT  NOT NULL,
    year       SMALLINT  NOT NULL,
    weekday    SMALLINT  NOT NULL
);

CREATE TABLE dimArea (
    area_id    VARCHAR   NOT NULL PRIMARY KEY SORTKEY,
    name       VARCHAR   NOT NULL,
    country    VARCHAR   NOT NULL
);

CREATE TABLE factNewCase (
    date_id    DATE     NOT NULL PRIMARY KEY SORTKEY,
    area_id    VARCHAR  NOT NULL DISTKEY,
    new_cases  INT  NOT NULL,
    FOREIGN KEY (date_id) REFERENCES dimTime(date_id),
    FOREIGN KEY (area_id) REFERENCES dimArea(area_id)
);
