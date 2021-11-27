from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class CreateStagingTableOperator(BaseOperator):

    QUERY_TEMPLATE = """
            BEGIN; END;
            SET json_serialization_enable TO TRUE;
            CREATE EXTERNAL TABLE s3_schema.{table_name}(
                {data_structure})
            ROW FORMAT SERDE '{file_format}'
            WITH SERDEPROPERTIES ({file_properties})
            STORED AS TEXTFILE
            LOCATION 's3://dev-udacity-capstone-project/raw_data/{table_name}/'
            TABLE PROPERTIES ('skip.header.line.count'='1');
        """

    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 file_structure,
                 file_format,
                 file_properties,
                 *args, **kwargs):
        super(CreateStagingTableOperator, self).__init__(*args, **kwargs)
        self.postgres_hook = PostgresHook(redshift_conn_id)
        self.table_name = table_name
        self.file_structure = file_structure
        self.file_format = file_format
        self.file_properties = file_properties

    def execute(self, context):
        self.log.info(f'Started creating staging table {self.table_name} as external table.')
        query = self.QUERY_TEMPLATE.format(**{
            'table_name': self.table_name,
            'data_structure': ', '.join([f'{field} {datatype}' for field, datatype in self.file_structure.items()]),
            'file_format': self.file_format,
            'file_properties': self.file_properties
        })
        self.postgres_hook.run(query)
        self.log.info(f'Finished creating staging table {self.table_name} as external table.')
