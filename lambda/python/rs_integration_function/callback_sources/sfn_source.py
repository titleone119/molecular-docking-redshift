from typing import List

from callback_sources.helper import CallbackSource
from redshift_data.finished_event import FinishedEvent
from statement_class import StatementName
from step_function.api import StepFunctionAPI


class SfnCallback(CallbackSource):
    def send_success(self, statement_name: StatementName, event_details: FinishedEvent):
        StepFunctionAPI.send_task_success(self.get_task_token(), event_details)

    def send_failure(self, statement_name: StatementName, event_details: FinishedEvent):
        StepFunctionAPI.send_task_failure(self.get_task_token(), event_details)

    TASK_TOKEN = "taskToken"
    EXECUTION_ARN = "executionArn"

    @classmethod
    def get_id_name(cls) -> str:
        return cls.EXECUTION_ARN

    @classmethod
    def get_callback_fieldnames(cls) -> List[str]:
        return [cls.TASK_TOKEN, cls.EXECUTION_ARN]

    def get_task_token(self):
        return self[self.TASK_TOKEN]