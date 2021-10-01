# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import json
from logger import logger, l_record, l_task_timed_out, l_task_token, l_item
from redshift_data.finished_event import FinishedEvent


class StepFunctionAPI(object):
    client = boto3.client('stepfunctions')

    @classmethod
    def send_task_success(cls, task_token: str, finished_event_details: dict):
        logger.debug({l_task_token: task_token, l_item: finished_event_details})
        try:
            cls.client.send_task_success(
                taskToken=task_token,
                output=json.dumps(finished_event_details)
            )
        except cls.client.exceptions.TaskTimedOut as tto:
            # TaskTimedOut means task has already timed out or has been completed previously.
            logger.warn({
                l_record: finished_event_details,
                l_task_timed_out: tto
            })

    @classmethod
    def send_task_failure(cls, task_token: str, finished_event_details: dict):
        logger.debug({l_task_token: task_token, l_item: finished_event_details})
        try:
            cls.client.send_task_failure(
                taskToken=task_token,
                error=FinishedEvent.QUERY_FAILED,
                cause=json.dumps(finished_event_details)
            )
        except cls.client.exceptions.TaskTimedOut as tto:
            # TaskTimedOut means task has already timed out or has been completed previously.
            logger.warn({
                l_record: finished_event_details,
                l_task_timed_out: tto
            })
