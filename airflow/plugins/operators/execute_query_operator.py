from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class ExecuteQueryOperator(BaseOperator):

    def __init__(self,
                 redshift_conn_id,
                 sql_query,
                 *args, **kwargs):
        super(ExecuteQueryOperator, self).__init__(*args, **kwargs)
        self.postgres_hook = PostgresHook(redshift_conn_id)
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info('Started loading data to table.')
        self.postgres_hook.run(self.sql_query)
        self.log.info('Finished loading data to table.')
