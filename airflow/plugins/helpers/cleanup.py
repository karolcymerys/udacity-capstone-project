import os

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

print('I was invoked.1')
AWS_CREDENTIALS = 'AWS_CREDENTIALS'
REDSHIFT_CONNECTION = 'REDSHIFT_CONNECTION'
AWS_REGION = os.environ['AWS_REGION']
ENVIRONMENT_NAME = os.environ['ENVIRONMENT_NAME']
print('I was invoked.2')


def cleanup(context):
    print('I was invoked.')
    postgres_hook = PostgresHook(REDSHIFT_CONNECTION)
    aws_hook = AwsBaseHook(aws_conn_id=AWS_CREDENTIALS, client_type='s3')
    s3_client = aws_hook.get_client_type(client_type='s3', region_name=AWS_REGION)

    __remove_table(postgres_hook, 'uk_source')
    __remove_table(postgres_hook, 'usa_source')
    __clear_s3(s3_client)


def __remove_table(postgres_hook, table):
    postgres_hook.run("""
            BEGIN; END;
            DROP TABLE IF EXISTS s3_schema.{table_name};
        """.format(**{'table_name': table}))


def __clear_s3(s3_client):
    files = s3_client.list_objects(
        Bucket=f'{ENVIRONMENT_NAME}-udacity-capstone-project',
        Prefix='raw_data'
    ).get('Contents', [])

    for file in files:
        s3_client.delete_object(
            Bucket=f'{ENVIRONMENT_NAME}-udacity-capstone-project',
            Key=file['Key']
        )
