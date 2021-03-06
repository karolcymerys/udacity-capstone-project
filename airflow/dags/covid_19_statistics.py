import datetime
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from helpers.cleanup import cleanup
from helpers.link_templates import LinkTemplates
from helpers.sql_queries import SQLQueries
from operators.create_staging_table_operator import CreateStagingTableOperator
from operators.data_quality_operator import DataQualityOperator
from operators.execute_query_operator import ExecuteQueryOperator
from operators.source_stream_operator import StreamSourcesOperator
from sensors.queue_state_sensor import QueueStateSensor

AWS_CREDENTIALS = 'AWS_CREDENTIALS'
REDSHIFT_CONNECTION = 'REDSHIFT_CONNECTION'

AWS_REGION = os.environ['AWS_REGION']
ENVIRONMENT_NAME = os.environ['ENVIRONMENT_NAME']
TOPIC_ARN = os.environ['TOPIC_ARN']

DATE_RANGE = ('2020-04-22', datetime.datetime.now().strftime('%Y-%m-%d'))

default_args = {
    'owner': 'aiflow',
    'depends_on_past': False,
    'start_date': datetime.datetime.now(),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=1)
}


with DAG('covid_19_statistics',
         default_args=default_args,
         description='ETL Data Pipeline for COVID-19 statistics',
         on_failure_callback=cleanup,
         on_success_callback=cleanup) as dag:
    start_operator = DummyOperator(task_id='start')

    with TaskGroup(group_id='stream_sources') as stream_sources_group:
        uk_data_stream_operator = StreamSourcesOperator(task_id='stream_links_for_uk',
                                                        aws_conn_id=AWS_CREDENTIALS,
                                                        aws_region=AWS_REGION,
                                                        source_name='uk_source',
                                                        target_arn=TOPIC_ARN,
                                                        date_range=DATE_RANGE,
                                                        link_template=LinkTemplates.UK_CASES_SOURCE,
                                                        data_format='json')

        usa_data_stream_operator = StreamSourcesOperator(task_id='stream_links_for_usa',
                                                         aws_conn_id=AWS_CREDENTIALS,
                                                         aws_region=AWS_REGION,
                                                         source_name='usa_source',
                                                         target_arn=TOPIC_ARN,
                                                         date_range=('2021-01-01', '2021-01-01'),
                                                         link_template=LinkTemplates.USA_CASES_SOURCE,
                                                         data_format='csv')

    queues_state_sensor = QueueStateSensor(task_id='wait_for_queue_to_be_empty',
                                           poke_interval=30,
                                           timeout=30 * 60,
                                           queues_name_prefix=ENVIRONMENT_NAME,
                                           aws_conn_id=AWS_CREDENTIALS,
                                           aws_region=AWS_REGION)

    with TaskGroup(group_id='staging_tables_creation') as staging_tables_creation:
        uk_create_external_table_operator = CreateStagingTableOperator(task_id='create_staging_table_for_uk',
                                                                       redshift_conn_id=REDSHIFT_CONNECTION,
                                                                       sql_query=SQLQueries.UK_STAGING_TABLE,
                                                                       environment_name=ENVIRONMENT_NAME)

        usa_create_external_table_operator = CreateStagingTableOperator(task_id='create_staging_table_for_usa',
                                                                        redshift_conn_id=REDSHIFT_CONNECTION,
                                                                        sql_query=SQLQueries.USA_STAGING_TABLE,
                                                                        environment_name=ENVIRONMENT_NAME)

    with TaskGroup(group_id='data_quality_check') as data_quality_check:
        uk_data_quality_check_operator = DataQualityOperator(task_id='data_quality_check_for_uk',
                                                             redshift_conn_id=REDSHIFT_CONNECTION,
                                                             test=SQLQueries.UK_DATA_QUALITY_CHECK)

        usa_data_quality_check_operator = DataQualityOperator(task_id='data_quality_check_for_usa',
                                                              redshift_conn_id=REDSHIFT_CONNECTION,
                                                              test=SQLQueries.USA_DATA_QUALITY_CHECK)

    with TaskGroup(group_id='loading_data') as loading_data:
        uk_loading_data_to_fact_table = ExecuteQueryOperator(task_id='loading_data_to_fact_table_for_uk',
                                                             redshift_conn_id=REDSHIFT_CONNECTION,
                                                             sql_query=SQLQueries.UK_LOAD_DATA_TO_FACT_TABLE)
        uk_loading_data_to_dim_time_table = ExecuteQueryOperator(task_id='loading_data_to_dim_time_table_for_uk',
                                                                 redshift_conn_id=REDSHIFT_CONNECTION,
                                                                 sql_query=SQLQueries.UK_LOAD_DATA_TO_DIM_TIME_TABLE)
        uk_loading_data_to_dim_area_table = ExecuteQueryOperator(task_id='loading_data_to_dim_area_table_for_uk',
                                                                 redshift_conn_id=REDSHIFT_CONNECTION,
                                                                 sql_query=SQLQueries.UK_LOAD_DATA_TO_DIM_REGION_TABLE)
        uk_loading_data_to_fact_table >> uk_loading_data_to_dim_area_table
        uk_loading_data_to_fact_table >> uk_loading_data_to_dim_time_table

        usa_loading_data_to_fact_table = ExecuteQueryOperator(task_id='loading_data_to_fact_table_for_usa',
                                                              redshift_conn_id=REDSHIFT_CONNECTION,
                                                              sql_query=SQLQueries.USA_LOAD_DATA_TO_FACT_TABLE)
        usa_loading_data_to_dim_time_table = ExecuteQueryOperator(task_id='loading_data_to_dim_time_table_for_usa',
                                                                  redshift_conn_id=REDSHIFT_CONNECTION,
                                                                  sql_query=SQLQueries.USA_LOAD_DATA_TO_DIM_TIME_TABLE)
        usa_loading_data_to_dim_area_table = ExecuteQueryOperator(task_id='loading_data_to_dim_area_table_for_usa',
                                                                  redshift_conn_id=REDSHIFT_CONNECTION,
                                                                  sql_query=SQLQueries.USA_LOAD_DATA_TO_DIM_REGION_TABLE)
        usa_loading_data_to_fact_table >> usa_loading_data_to_dim_time_table
        usa_loading_data_to_fact_table >> usa_loading_data_to_dim_area_table

    with TaskGroup(group_id='handling_duplicates_in_dim_tables') as handling_duplicates_in_dim_tables:
        dim_time_handle_duplicates_operator = ExecuteQueryOperator(task_id='dim_time_handle_duplicates',
                                                                   redshift_conn_id=REDSHIFT_CONNECTION,
                                                                   sql_query=SQLQueries.DIM_TIME_REMOVE_DUPLICATES)

        dim_region_handle_duplicates_operator = ExecuteQueryOperator(task_id='dim_region_handle_duplicates',
                                                                     redshift_conn_id=REDSHIFT_CONNECTION,
                                                                     sql_query=SQLQueries.DIM_REGION_REMOVE_DUPLICATES)

    end_operator = DummyOperator(task_id='finish_execution', dag=dag)

    start_operator >> stream_sources_group >> queues_state_sensor >> staging_tables_creation
    staging_tables_creation >> data_quality_check >> loading_data
    loading_data >> handling_duplicates_in_dim_tables >> end_operator
