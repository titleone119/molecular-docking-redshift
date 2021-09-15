import json


class FinishedEvent(dict):
    QUERY_FINISHED = "FINISHED"
    QUERY_FAILED = "FAILED"
    QUERY_ABORTED = "ABORTED"
    
    def __init__(self, input_dict):
        super(FinishedEvent, self).__init__(input_dict)

    @classmethod
    def from_record(cls, record: dict):
        finished_event_details_str = record['body']
        finished_event_details = json.loads(finished_event_details_str)
        return cls(finished_event_details)

    def get_execution_detail(self) -> dict:
        return self['detail']

    def get_state(self) -> str:
        return self.get_execution_detail()['state']

    def get_statement_name(self) -> str:
        return self.get_execution_detail()['statementName']

    def has_failed(self):
        return self.get_state() in (self.QUERY_FAILED, self.QUERY_ABORTED)

    def has_succeeded(self):
        return self.get_state() == self.QUERY_FINISHED
