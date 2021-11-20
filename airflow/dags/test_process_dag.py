import json
import datetime
from typing import Any, Dict

import boto3
from airflow import DAG
from airflow.models import BaseOperator, Connection
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.sns import AwsSnsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import BaseSensorOperator


class StreamSourcesOperator(BaseOperator):

    def __init__(self,
                 aws_conn_id,
                 source_name,
                 target_arn,
                 _start_date,
                 _end_date,
                 link_template,
                 date_format,
                 *args, **kwargs):
        super(StreamSourcesOperator, self).__init__(*args, **kwargs)
        self.aws_sns_hook = AwsSnsHook(aws_conn_id=aws_conn_id, region_name='eu-west-1')
        self.source_name = source_name
        self.target_arn = target_arn
        self.step_start_date = datetime.datetime.strptime(_start_date, '%Y-%m-%d'),
        self.step_end_date = datetime.datetime.strptime(_end_date, '%Y-%m-%d')
        self.link_template = link_template
        self.date_format = date_format

    def execute(self, context: Any):
        self.log.info(f'Starting sending links for source {self.source_name}')
        delta = self.step_end_date - self.step_start_date[0]
        dates = [self.step_start_date[0] + datetime.timedelta(days=d) for d in range(delta.days + 1)]
        messages = [{
            'source': self.link_template % date.strftime(self.date_format),
            'destination': f'raw_data/{self.source_name}/{date.year}_{date.month}_{date.day}_data.csv'
        } for date in dates]

        for message in messages:
            self.aws_sns_hook.publish_to_target(
                target_arn=self.target_arn,
                message=json.dumps(message),
                message_attributes={
                    'source_name': self.source_name
                }
            )
        self.log.info(f'All {len(messages)} links for source {self.source_name} have been sent.')


class QueueStateSensor(BaseSensorOperator):

    def __init__(self,
                 environment_name,
                 aws_conn_id,
                 *args, **kwargs):
        super(QueueStateSensor, self).__init__(*args, **kwargs)
        conn = Connection.get_connection_from_secrets(aws_conn_id)
        self.sqs_client = boto3.client('sqs',
                                       region_name='eu-west-1',
                                       aws_access_key_id=conn.login,
                                       aws_secret_access_key=conn.password)
        self.environment = environment_name

    def poke(self, context: Dict) -> bool:
        queues = self.sqs_client.list_queues(QueueNamePrefix=f'{self.environment}-')
        for queue_url in queues['QueueUrls']:
            if not self._is_queue_empty(queue_url):
                return False

        return True

    def _is_queue_empty(self, queue_url):
        queue_attributes = self.sqs_client. \
            get_queue_attributes(QueueUrl=queue_url,
                                 AttributeNames=['ApproximateNumberOfMessagesDelayed','ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessages'])['Attributes']
        self.log.info(queue_attributes)
        return int(queue_attributes['ApproximateNumberOfMessagesDelayed']) == 0 and \
               int(queue_attributes['ApproximateNumberOfMessagesNotVisible']) == 0 and \
               int(queue_attributes['ApproximateNumberOfMessages']) == 0


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
                "date" DATE,
                newCases int,
                areaName varchar,
                areaCode varchar,
                cumulativeNewCases int)
            row format delimited fields terminated by ','
            stored as textfile
            location 's3://dev-udacity-capstone-project/raw_data/{table_name}/'
            TABLE PROPERTIES ('skip.header.line.count'='1');
        """.format(**{'table_name': self.table_name}))
        self.log.info(f'External table created')


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


with DAG('test_process_dag',
         description='ETL Process to extract, transform, load covid data',
         start_date=datetime.datetime.now()) as dag:

    start_operator = DummyOperator(task_id='start_execution',  dag=dag)

    streaming_operator = StreamSourcesOperator(task_id='stream_link_sources_for_source_1', dag=dag,
                                               aws_conn_id='aws_credentials',
                                               source_name='source1',
                                               target_arn='arn:aws:sns:eu-west-1:534172043736:dev-sns-topic',
                                               _start_date='2021-8-1',
                                               _end_date='2021-11-16',
                                               link_template='https://api.coronavirus.data.gov.uk/v1/data?filters=areaType=ltla;date=%s&structure={"date":"date","newCases":"newCasesByPublishDate", "areaName": "areaName", "areaCode": "areaCode", "cumulativeNewCases": "cumCasesByPublishDate"}&format=csv',
                                               date_format='%Y-%m-%d',
                                               )

    empty_queues_sensor = QueueStateSensor(task_id='wait_for_queue_to_be_empty',
                                           poke_interval=30,
                                           timeout=30*60,
                                           environment_name='dev',
                                           aws_conn_id='aws_credentials')

    create_external_table_operator = CreateExternalTableOperator(task_id='create_external_table_for_source_1', dag=dag,
                                                                 redshift_conn_id='redshift_connection',
                                                                 table_name='source1')

    delete_external_table_operator = DeleteExternalTableOperator(task_id='delete_external_table_for_source_1', dag=dag,
                                                                 redshift_conn_id='redshift_connection',
                                                                 table_name='source1')

    end_operator = DummyOperator(task_id='finish_execution',  dag=dag)

    start_operator >> streaming_operator >> empty_queues_sensor >> create_external_table_operator >> delete_external_table_operator >> end_operator
