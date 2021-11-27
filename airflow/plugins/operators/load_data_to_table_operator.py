from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadDataTableOperator(BaseOperator):

    SQL_TEMPLATE = '''
        INSERT INTO {table_name}
            {select_statement}
        '''

    def __init__(self,
                 redshift_conn_id,
                 select_query,
                 table_name,
                 *args, **kwargs):
        super(LoadDataTableOperator, self).__init__(*args, **kwargs)
        self.postgres_hook = PostgresHook(redshift_conn_id)
        self.select_query = select_query
        self.table_name = table_name

    def execute(self, context):
        self.log.info(f'Started loading data to table {self.table_name}.')
        query = self.SQL_TEMPLATE.format(**{
            'table_name': self.table_name,
            'select_statement': self.select_query
        })
        self.postgres_hook.run(query)
        self.log.info(f'Finished loading data to table {self.table_name}.')
