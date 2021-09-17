from typing import List, Union

from callback_sources.helper import CallbackSource
from redshift_data.finished_event import FinishedEvent
from statement_class import StatementName
import callback_sources.cfnresponse as cfn_response


class CfnCallback(CallbackSource):
    """
    CfnCallback is a callback integration that allows notifying Cloudformation when a Redshift operation has finished.
    The event that should trigger the SfnRedshiftTasker lambdaFunction must have the required fields that Cloudformation
    populates when creating a custom resource. See AWS docs for details:
    https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/crpg-ref-requests.html#crpg-ref-request-fields

    The easiest way of using this callback is by using the event emitted by AWS Cloudformation and add a 'sqlStatement'
    field to the event that contains the SQL statement. This SQL statement can be built using the provided event details
    specifically the RequestType and ResourceProperties allow to dynamically build SQL statements.
    """
    REQUEST_TYPE = "RequestType"
    RESPONSE_URL = "ResponseURL"
    STACK_ID = "StackId"
    REQUEST_ID = "RequestId"
    RESOURCE_TYPE = "ResourceType"
    LOGICAL_RESOURCE_ID = "LogicalResourceId"
    PHYSICAL_RESOURCE_ID = "PhysicalResourceId"
    OLD_RESOURCE_PROPERTIES = "OldResourceProperties"
    SERVICE_TOKEN = "ServiceToken"
    RESOURCE_PROPERTIES = "ResourceProperties"

    MANDATORY_FOR_UPDATE = [PHYSICAL_RESOURCE_ID, OLD_RESOURCE_PROPERTIES]
    MANDATORY_FOR_DELETE = [PHYSICAL_RESOURCE_ID]
    OPTIONAL_FIELDS = [SERVICE_TOKEN, RESOURCE_PROPERTIES]

    REQUEST_UPDATE = 'Update'
    REQUEST_DELETE = 'Delete'

    def __init__(self, input_dict: dict):
        super(CfnCallback, self).__init__(input_dict)
        for field_name in self.OPTIONAL_FIELDS:
            if field_name in input_dict:
                self[field_name] = input_dict[field_name]
        if self[self.REQUEST_TYPE] == self.REQUEST_UPDATE:
            for field_name in self.MANDATORY_FOR_UPDATE:
                self[field_name] = input_dict[field_name]
        elif self[self.REQUEST_TYPE] == 'Delete':
            for field_name in self.MANDATORY_FOR_DELETE:
                self[field_name] = input_dict[field_name]

    @classmethod
    def get_id_name(cls) -> Union[str, None]:
        return cls.LOGICAL_RESOURCE_ID

    @classmethod
    def get_callback_fieldnames(cls) -> List[str]:
        return [cls.REQUEST_TYPE, cls.RESPONSE_URL, cls.STACK_ID, cls.REQUEST_ID, cls.RESOURCE_TYPE,
                cls.LOGICAL_RESOURCE_ID]

    def send_result(self, statement_name: StatementName, event_details: FinishedEvent, result: str, reason: str):
        if self[self.REQUEST_TYPE] in ['Update', 'Delete']:
            # When manipulating an existing resource we have to re-use the physical resource ID because we modify
            # The existing database user rather than creating a new one (since username must be unique)
            physical_id = self[self.PHYSICAL_RESOURCE_ID]
        else:
            physical_id = str(statement_name)
        cfn_response.send(self, result, physical_id, reason, event_details)

    def send_success(self, statement_name: StatementName, event_details: FinishedEvent):
        self.send_result(statement_name, event_details, cfn_response.SUCCESS, "succes")

    def send_failure(self, statement_name: StatementName, event_details: FinishedEvent):
        self.send_result(statement_name, event_details, cfn_response.FAILED, f"See {event_details}")
