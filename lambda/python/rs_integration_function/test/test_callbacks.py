import os

import pytest as pytest

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