import datetime
import json

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.sns import AwsSnsHook


class StreamSourcesOperator(BaseOperator):

    def __init__(self,
                 aws_conn_id,
                 aws_region,
                 source_name,
                 target_arn,
                 first_date_to_import,
                 last_date_to_import,
                 link_template,
                 source_date_format,
                 *args, **kwargs):
        super(StreamSourcesOperator, self).__init__(*args, **kwargs)
        self.aws_sns_hook = AwsSnsHook(aws_conn_id=aws_conn_id, region_name=aws_region)
        self.source_name = source_name
        self.target_arn = target_arn
        self.step_start_date = datetime.datetime.strptime(first_date_to_import, '%Y-%m-%d'),
        self.step_end_date = datetime.datetime.strptime(last_date_to_import, '%Y-%m-%d')
        self.link_template = link_template
        self.source_date_format = source_date_format

    def execute(self, context):
        self.log.info(f'Starting streaming links for source {self.source_name}.')
        dates = self.__get_all_dates()
        messages = self.__build_messages(dates)
        self.__send_messages(messages)
        self.log.info(f'Finishing. All {len(messages)} links for source {self.source_name} have been sent.')

    def __get_all_dates(self):
        delta = self.step_end_date - self.step_start_date[0]
        return [self.step_start_date[0] + datetime.timedelta(days=d) for d in range(delta.days + 1)]

    def __build_messages(self, dates):
        return [{
            'source': self.link_template % date.strftime(self.source_date_format),
            'destination': f'raw_data/{self.source_name}/{date.year}_{date.month}_{date.day}_data.csv'
        } for date in dates]

    def __send_messages(self, messages):
        for message in messages:
            self.log.info(f'Sending message {message}.')
            self.aws_sns_hook.publish_to_target(
                target_arn=self.target_arn,
                message=json.dumps(message),
                message_attributes={
                    'source_name': self.source_name
                }
            )