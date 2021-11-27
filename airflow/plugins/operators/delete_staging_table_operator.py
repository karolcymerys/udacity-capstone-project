from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DeleteStagingTableOperator(BaseOperator):

    QUERY_TEMPLATE = """
            BEGIN; END;
            DROP TABLE IF EXISTS s3_schema.{table_name};
        """

    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 *args, **kwargs):
        super(DeleteStagingTableOperator, self).__init__(*args, **kwargs)
        self.postgres_hook = PostgresHook(redshift_conn_id)
        self.table_name = table_name

    def execute(self, context):
        self.log.info(f'Started deleting staging table {self.table_name}')
        query = self.QUERY_TEMPLATE.format(**{
            'table_name': self.table_name
        })
        self.postgres_hook.run(query)
        self.log.info(f'Finished deleting staging table {self.table_name}')
