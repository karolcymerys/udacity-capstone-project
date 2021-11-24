import datetime
from typing import Any

import boto3
from airflow import DAG
from airflow.models import BaseOperator, Connection
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from operators.source_stream_operator import StreamSourcesOperator
from sensors.queue_state_sensor import QueueStateSensor

AWS_REGION = 'eu-west-1'


class CreateExternalTableOperator(BaseOperator):

    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 *args, **kwargs):
        super(CreateExternalTableOperator, self).__init__(*args, **kwargs)
        self.postgres_hook = PostgresHook(redshift_conn_id)
        self.table_name = table_name

    def execute(self, context: Any):
        self.log.info(f'External table creation started')
        self.postgres_hook.run("""
            BEGIN; END;
            CREATE EXTERNAL TABLE s3_schema.{table_name}(
                dateTime DATE,
                newCases int,
                areaName varchar,
                areaCode varchar,
                cumulativeNewCases int)
            row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            with serdeproperties (
              'separatorChar' = ',',
              'quoteChar' = '\"',
              'escapeChar' = '\\\\'
            )
            stored as textfile
            location 's3://dev-udacity-capstone-project/raw_data/{table_name}/'
            TABLE PROPERTIES ('skip.header.line.count'='1');
        """.format(**{'table_name': self.table_name}))
        self.log.info(f'External table created')


class CreateFactTableOperator(BaseOperator):

    def __init__(self,
                 redshift_conn_id,
                 aws_conn_id,
                 *args, **kwargs):
        super(CreateFactTableOperator, self).__init__(*args, **kwargs)
        self.postgres_hook = PostgresHook(redshift_conn_id)
        conn = Connection.get_connection_from_secrets(aws_conn_id)
        self.s3_client = boto3.client('s3',
                                      region_name='eu-west-1',
                                      aws_access_key_id=conn.login,
                                      aws_secret_access_key=conn.password)

    def execute(self, context: Any):
        self.log.info(f'Fact table creation started')
        self.postgres_hook.run("""
            unload('SELECT 
                extract(''epoch'' from file.dateTime) as date_id,
                file.areaCode as area_id,
                file.newCases as new_cases 
            FROM s3_schema.source1 file')
            to 's3://dev-udacity-capstone-project/fact_tables/covid_stats/'
            iam_role 'arn:aws:iam::534172043736:role/dev-redshift-role'
            FORMAT as PARQUET
            PARTITION BY ( date_id);
        """)
        self.log.info(f'Fact table creation finished')

        self.log.info(f'Adding partitions started')
        partitions = \
        self.s3_client.list_objects(Bucket='dev-udacity-capstone-project', Prefix='fact_tables/covid_stats/date_id')[
            'Contents']
        for partition in partitions:
            partition_value = partition['Key'].split('/')[2].split('=')[1]
            self.postgres_hook.run(f"""
                        BEGIN;END;
                        alter table s3_schema.factCovid_stats
                            add if not exists partition (date_id='{partition_value}') 
                            location 's3://dev-udacity-capstone-project/fact_tables/covid_stats/date_id={partition_value}';
                    """)
        self.log.info(f'Adding partitions finished')


class CreateDim1TableOperator(BaseOperator):

    def __init__(self,
                 redshift_conn_id,
                 *args, **kwargs):
        super(CreateDim1TableOperator, self).__init__(*args, **kwargs)
        self.postgres_hook = PostgresHook(redshift_conn_id)

    def execute(self, context: Any):
        self.log.info(f'Fact table dimTime creation started')
        self.postgres_hook.run("""
            insert into dimTime
                SELECT distinct 
                    extract('epoch' from file.dateTime) as date_id,
                    EXTRACT(day FROM file.dateTime) as day,
                    EXTRACT(week FROM file.dateTime) as week,
                    EXTRACT(month FROM file.dateTime) as month,
                    EXTRACT(year FROM file.dateTime) as year,
                    EXTRACT(dayofweek FROM file.dateTime) as weekday
                FROM s3_schema.source1 file
        """)
        self.log.info(f'Fact table dimTime creation finished')


class CreateDim2TableOperator(BaseOperator):

    def __init__(self,
                 redshift_conn_id,
                 *args, **kwargs):
        super(CreateDim2TableOperator, self).__init__(*args, **kwargs)
        self.postgres_hook = PostgresHook(redshift_conn_id)

    def execute(self, context: Any):
        self.log.info(f'Fact table dimTime creation started')
        self.postgres_hook.run("""
            insert into dimArea
                SELECT distinct 
                    file.areaCode as area_id,
                    file.areaName as city,
                    case when SUBSTRING(file.areaCode, 1, 1) = 'E' then 'England'
                         when SUBSTRING(file.areaCode, 1, 1) = 'N' then 'Northern Ireland'
                         when SUBSTRING(file.areaCode, 1, 1) = 'S' then 'Scotland'
                         when SUBSTRING(file.areaCode, 1, 1) = 'W' then 'Wales'
                    end as country
                FROM s3_schema.source1 file
        """)
        self.log.info(f'Fact table dimTime creation finished')


class DeleteExternalTableOperator(BaseOperator):

    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 *args, **kwargs):
        super(DeleteExternalTableOperator, self).__init__(*args, **kwargs)
        self.postgres_hook = PostgresHook(redshift_conn_id)
        self.table_name = table_name

    def execute(self, context: Any):
        self.log.info(f'External table deletion started')
        self.postgres_hook.run("""
            BEGIN; END;
            DROP TABLE IF EXISTS s3_schema.{table_name};
        """.format(**{'table_name': self.table_name}))
        self.log.info(f'External table deleted')


with DAG('covid_metrics_etl',
         description='ETL process to extract, transform, load covid metrics',
         start_date=datetime.datetime.now()) as dag:
    start_operator = DummyOperator(task_id='start')

    with TaskGroup(group_id='stream_sources') as stream_sources_group:
        uk_data_stream_operator = StreamSourcesOperator(task_id='stream_data_for_uk',
                                                        aws_conn_id='aws_credentials',
                                                        aws_region=AWS_REGION,
                                                        source_name='source1',
                                                        target_arn='arn:aws:sns:eu-west-1:534172043736:dev-sns-topic',
                                                        first_date_to_import='2021-8-1',
                                                        last_date_to_import='2021-11-16',
                                                        link_template='https://api.coronavirus.data.gov.uk/v1/data?filters='
                                                                      'areaType=ltla;'
                                                                      'date=%s'
                                                                      '&structure={"date":"date",'
                                                                      '"newCases":"newCasesByPublishDate",'
                                                                      '"areaName": "areaName",'
                                                                      '"areaCode": "areaCode",'
                                                                      '"cumulativeNewCases": "cumCasesByPublishDate"}'
                                                                      '&format=csv',
                                                        source_date_format='%Y-%m-%d')


    queues_state_sensor = QueueStateSensor(task_id='wait_for_queue_to_be_empty',
                                           poke_interval=30,
                                           timeout=30 * 60,
                                           queues_name_prefix='dev',
                                           aws_conn_id='aws_credentials',
                                           aws_region=AWS_REGION)

    create_external_table_operator = CreateExternalTableOperator(task_id='create_external_table_for_source_1', dag=dag,
                                                                 redshift_conn_id='redshift_connection',
                                                                 table_name='source1')

    import_fact_data = CreateFactTableOperator(task_id='import_fact_data_for_source_1', dag=dag,
                                               redshift_conn_id='redshift_connection', aws_conn_id='aws_credentials')

    import_dim_1_data = CreateDim1TableOperator(task_id='import_dim1_data_for_source_1', dag=dag,
                                                redshift_conn_id='redshift_connection')

    import_dim_2_data = CreateDim2TableOperator(task_id='import_dim2_data_for_source_1', dag=dag,
                                                redshift_conn_id='redshift_connection')

    delete_external_table_operator = DeleteExternalTableOperator(task_id='delete_external_table_for_source_1', dag=dag,
                                                                 redshift_conn_id='redshift_connection',
                                                                 table_name='source1')

    end_operator = DummyOperator(task_id='finish_execution', dag=dag)

    start_operator >> stream_sources_group >> queues_state_sensor >> create_external_table_operator >> import_fact_data
    import_fact_data >> import_dim_1_data >> delete_external_table_operator
    import_fact_data >> import_dim_2_data >> delete_external_table_operator
    delete_external_table_operator >> end_operator
