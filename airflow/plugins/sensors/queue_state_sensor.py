import boto3
from airflow.models import Connection
from airflow.sensors.base import BaseSensorOperator


class QueueStateSensor(BaseSensorOperator):

    def __init__(self,
                 queues_name_prefix,
                 aws_conn_id,
                 aws_region,
                 *args, **kwargs):
        super(QueueStateSensor, self).__init__(*args, **kwargs)
        self.queues_name_prefix = queues_name_prefix
        conn = Connection.get_connection_from_secrets(aws_conn_id)
        self.sqs_client = boto3.client('sqs',
                                       region_name=aws_region,
                                       aws_access_key_id=conn.login,
                                       aws_secret_access_key=conn.password)

    def poke(self, context) -> bool:
        self.log.info(f'Starting checking states of queues with prefix {self.queues_name_prefix}')
        queues = self.__search_queues_with_prefix_name(self.queues_name_prefix)
        for queue_url in queues['QueueUrls']:
            if not self._is_queue_empty(queue_url):
                self.log.info(
                    f'Finishing checking states of queues with prefix {self.queues_name_prefix}. '
                    f'Queue {queue_url} is not empty.')
                return False

        self.log.info(f'Finishing checking states of queues with prefix {self.queues_name_prefix}. All queues are empty.')
        return True

    def __search_queues_with_prefix_name(self, prefix_name):
        return self.sqs_client.list_queues(QueueNamePrefix=prefix_name).get('QueueUrls', [])

    def _is_queue_empty(self, queue_url):
        queue_attributes = self.sqs_client. \
            get_queue_attributes(QueueUrl=queue_url,
                                 AttributeNames=['ApproximateNumberOfMessagesDelayed',
                                                 'ApproximateNumberOfMessagesNotVisible',
                                                 'ApproximateNumberOfMessages'])['Attributes']
        self.log.info(queue_attributes)
        return int(queue_attributes['ApproximateNumberOfMessagesDelayed']) == 0 and \
               int(queue_attributes['ApproximateNumberOfMessagesNotVisible']) == 0 and \
               int(queue_attributes['ApproximateNumberOfMessages']) == 0
