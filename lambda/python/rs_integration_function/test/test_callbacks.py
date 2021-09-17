import os

import pytest as pytest

from callback_sources.cfn_callback import CfnCallback
from callback_sources.helper import NoCallback
from callback_sources.sfn_source import SfnCallback
from event_labels import TASK_TOKEN, EXECUTION_ARN, SQL_STATEMENT


def initialize_test_env():
    os.environ['DDB_TABLE_NAME'] = 'Dummy'
    os.environ['TTL'] = '1'
    os.environ['CLUSTER_IDENTIFIER'] = 'DummyCluster'
    os.environ['DATABASE'] = 'DummyDB'
    os.environ['DB_USER'] = 'DummyUser'
    return os.environ


@pytest.fixture()
def callback_source_builder():
    initialize_test_env()
    from callback_sources.builder import CallbackSourceBuilder
    return CallbackSourceBuilder


def test_simple_no_callback(callback_source_builder):
    event = {}
    assert callback_source_builder.get_callback_class_for_event(event) == NoCallback, "No callback should be used"


def test_sfn_callback(callback_source_builder):
    event = {
        TASK_TOKEN: "dummyToken",
        EXECUTION_ARN: "arn:dummy",
        SQL_STATEMENT: "select * from dummy"
    }
    assert callback_source_builder.get_callback_class_for_event(event) == SfnCallback, "SFN callback should returned"


def test_cfn_callback(callback_source_builder):
    event = {
        "RequestType": "Delete",
        "ServiceToken": "arn:aws:lambda:eu-west-1:...:function:integ-test-stack-RSUserManagerAC63661...",
        "ResponseURL": "https://cloudformation-custom-resource-response-euwest1.s3-eu-west-1.amazonaws.com/...",
        "StackId": "arn:aws:cloudformation:eu-west-1:...:stack/integ-test-stack/327607c0-c9e2-11eb-becd-0a67c69ba16d",
        "RequestId": "a60d7949-852f-4c7c-b16b-0f7764cca45e",
        "LogicalResourceId": "rscfncreateduser",
        "PhysicalResourceId": "rscfncreateduser:1631885785.837462",
        "ResourceType": "AWS::CloudFormation::CustomResource",
        "ResourceProperties": {
            "ServiceToken": "arn:aws:lambda:eu-west-1:...:function:integ-test-stack-RSUserManagerAC636618-sMEOJeyrNMmb",
            "username": "cfncreated_user"
        },
        "sqlStatement": "DROP USER IF EXISTS cfncreated_user;"
    }
    assert callback_source_builder.get_callback_class_for_event(event) == CfnCallback, "CFN callback should returned"
    assert isinstance(callback_source_builder.get_callback_object_for_event(event), CfnCallback)


def test_cfn_callback_with_list(callback_source_builder):
    event = {
        "RequestType": "Update",
        "ServiceToken": "arn:aws:lambda:eu-west-1:x:function:integ-test-stack-RSUserManagerAC636618-sMEOJeyrNMmb",
        "ResponseURL": "https://cloudformation-custom-resource-response-euwest1.s3-eu-west-1.amazonaws...",
        "StackId": "arn:aws:cloudformation:eu-west-1:x:stack/integ-test-stack/327607c0-c9e2-11eb-becd-0a67c69ba16d",
        "RequestId": "bc956190-d425-4669-8ca6-df1a19a1d647",
        "LogicalResourceId": "rscfncreateduser",
        "PhysicalResourceId": "rscfncreateduser:1631896619.417732",
        "ResourceType": "AWS::CloudFormation::CustomResource",
        "ResourceProperties": {
            "ServiceToken": "arn:aws:lambda:eu-west-1:x:function:integ-test-stack-RSUserManagerAC636618-sMEOJeyrNMmb",
            "username": "cfncreated_user"
        },
        "OldResourceProperties": {
            "ServiceToken": "arn:aws:lambda:eu-west-1:x:function:integ-test-stack-RSUserManagerAC636618-sMEOJeyrNMmb",
            "password": "e1c252bf4c426727db9c7bfc726760d8",
            "valid_until": "2025-01-01 12:00:00",
            "unrestricted_syslog_access": "true",
            "groups": [
                "groupA",
                "groupB"
            ],
            "create_user": "true",
            "create_db": "true",
            "session_timeout": "1728000",
            "username": "cfncreated_user",
            "connection_limit": "3"
        },
        "sqlStatement": "ALTER USER cfncreated_user PASSWORD DISABLE"
    }
    assert callback_source_builder.get_callback_class_for_event(event) == CfnCallback, "CFN callback should returned"
    assert isinstance(callback_source_builder.get_callback_object_for_event(event), CfnCallback)
