from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):

    def __init__(self,
                 redshift_conn_id,
                 test,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_hook = PostgresHook(redshift_conn_id)
        self.query = test['query']
        self.expected_result = test['expected_result']

    def execute(self, context):
        self.log.info(f'Test {self.query} started')
        records = self.postgres_hook.get_first(self.query)
        assert records[0] == self.expected_result, f"Test {self.query} has return invalid result: {records[0]}"
        self.log.info(f'Test {self.query} completed')
