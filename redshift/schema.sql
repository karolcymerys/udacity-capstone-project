CREATE EXTERNAL SCHEMA s3_schema
FROM DATA CATALOG
DATABASE 'udacity-capstone-project'
iam_role 'arn:aws:iam::534172043736:role/dev-redshift-role'
CREATE EXTERNAL database IF NOT EXISTS;
