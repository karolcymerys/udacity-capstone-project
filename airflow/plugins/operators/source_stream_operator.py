import datetime
import itertools
import json
from collections import ChainMap

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.sns import AwsSnsHook


class StreamSourcesOperator(BaseOperator):

    def __init__(self,
                 aws_conn_id,
                 aws_region,
                 target_arn,
                 date_range,
                 source_name,
                 link_template,
                 data_format,
                 source_date_format='%Y-%m-%d',
                 url_parameters={},
                 *args, **kwargs):
        super(StreamSourcesOperator, self).__init__(*args, **kwargs)
        self.aws_sns_hook = AwsSnsHook(aws_conn_id=aws_conn_id, region_name=aws_region)
        self.target_arn = target_arn

        self.step_start_date = datetime.datetime.strptime(date_range[0], '%Y-%m-%d')
        self.step_end_date = datetime.datetime.strptime(date_range[1], '%Y-%m-%d')

        self.source_name = source_name
        self.link_template = link_template
        self.source_date_format = source_date_format
        self.url_parameters = url_parameters
        self.data_format = data_format

    def execute(self, context):
        self.log.info(f'Starting streaming links for source {self.source_name}.')
        self.url_parameters.update({'date': self.__get_all_dates()})
        messages = self.__build_messages(self.url_parameters)
        self.__send_messages(messages)
        self.log.info(f'Finishing. All {len(messages)} links for source {self.source_name} have been sent.')

    def __get_all_dates(self):
        delta = (self.step_end_date - self.step_start_date)
        return [(self.step_start_date + datetime.timedelta(days=d)).strftime(self.source_date_format) for d in range(delta.days + 1)]

    def __build_messages(self, parameters):
        parameter_groups = []
        for param, values in parameters.items():
            parameter_groups.append([{param: value} for value in values])

        parameter_groups = [dict(ChainMap(*group)) for group in list(itertools.product(*parameter_groups))]

        return [{
            'source': self.link_template.format(**params),
            'destination': f'raw_data/{self.source_name}/{"_".join([ value for value in params.values()])}_data.{self.data_format}'
        } for params in parameter_groups]

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
