# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


import json
from datetime import datetime, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
import os

from callback_sources.builder import CallbackSourceBuilder
from callback_sources.helper import CallbackInterface, NoCallback
from exceptions import ConfigurationError, PreviousExecutionNotFound, NoTrackedState
from statement_class import StatementName
from ddb import DDB_ID, DDB_TABLE_NAME, DDB_TTL, DDB_FINISHED_EVENT_DETAILS, DDB_INVOCATION_ID, DDB_CALLBACK_DETAILS
from event_labels import SQL_STATEMENT
from assertion import assert_env_set
from logger import logger, l_statement_name, l_response, l_finished_event_details, l_ttl, l_item, l_exception

assert_env_set(DDB_TABLE_NAME)
dynamodb = boto3.resource('dynamodb')
ddb_state_table = dynamodb.Table(os.environ[DDB_TABLE_NAME])
try:
    ddb_ttl_in_days = int(os.environ[DDB_TTL])
except ValueError:
    raise ConfigurationError(f"{DDB_TTL} should be TTL in number of days that state is kept.")


class DDBStateTable(object):
    class StatementNotTrackedException(Exception):
        """Raised when trying to get state for a statement that is not tracked in this state table."""

    @classmethod
    def object_floats_to_decimal(cls, o):
        return json.loads(json.dumps(o), parse_float=Decimal)

    def update_item(self, *args, **kwargs):
        """
        Update an Item in DynamoDB. The API is the same as the Table Resource update_item but with the restriction that
        it should use the new UpdateExpression syntax and pass in attribute values using 'ExpressionAttributeValues'.

        Prior to calling the `update_item` API we replace floats with Decimals as DynamoDB does not allow floats.
        """
        assert 'ExpressionAttributeValues' in kwargs, 'We only support the none legacy Table Resource update_item!'
        expression_attribute_values = kwargs['ExpressionAttributeValues']
        cleaned_expression_attribute_values = {}
        for key, value in expression_attribute_values.items():
            cleaned_expression_attribute_values[key] = self.object_floats_to_decimal(value)
        kwargs['ExpressionAttributeValues'] = cleaned_expression_attribute_values
        return ddb_state_table.update_item(*args, **kwargs)

    def put_item(self, *args, **kwargs):
        """
        Put an Item in DynamoDB. The API is the same as the Table Resource put_item.

        Prior to calling the `put_item` API we replace floats with Decimals as DynamoDB does not allow floats.
        """
        assert 'Item' in kwargs, 'We use the Table Resource put_item so Item is required.'
        kwargs['Item'] = self.object_floats_to_decimal(kwargs['Item'])
        return ddb_state_table.put_item(*args, **kwargs)

    def register_execution_start(self, callback_object: CallbackInterface, sql_statement: str) -> StatementName:
        """
        Register a UUID4 string in a state table in DynamoDB and link it with the task of the stepfunction execution.
        Return this GUID string such that it can be used as statement name to update the task when the statement
        completes.
        """
        statement_name = StatementName.from_execution_arn(callback_object.get_id())
        item_details = {
            DDB_ID: statement_name.execution_arn,
            DDB_INVOCATION_ID: statement_name.invocation_id,
            SQL_STATEMENT: sql_statement,
            DDB_CALLBACK_DETAILS: callback_object.to_json()
        }
        if isinstance(callback_object, NoCallback):
            # No callback is expected so TTL can immediately be set.
            item_details[DDB_TTL] = self.get_ttl_value()
        logger.debug({l_item: item_details})
        self.put_item(
            Item=item_details,
            ConditionExpression="attribute_not_exists(sqlStatement)"  # Re-registration is  not allowed
        )
        return statement_name

    @classmethod
    def get_latest_statement_name_for_execution_arn(cls, execution_arn: str) -> StatementName:
        response = ddb_state_table.query(
            KeyConditionExpression=Key(DDB_ID).eq(execution_arn),
            ProjectionExpression="#I",
            ExpressionAttributeNames={
                "#I": DDB_INVOCATION_ID
            },
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE',
        )
        logger.debug({l_response: response})
        items = response['Items']
        if len(items) == 0:
            e = PreviousExecutionNotFound(f"No started statements found for {execution_arn}")
            logger.warning({l_exception: e, l_response: response}, stack_info=True)
            raise e
        return StatementName(
            execution_arn,
            invocation_id=str(max(float(i[DDB_INVOCATION_ID]) for i in items))
        )

    @classmethod
    def get_callback_source_for_statement_name(cls, statement_name: StatementName) -> CallbackInterface:
        """
        This is a very efficient DDB lookup which will only consume 1 RCU.
        Args:
            statement_name:

        Returns:
            The task token that requested issuing of this statement.
        """
        response = ddb_state_table.get_item(
            Key={
                DDB_ID: statement_name.execution_arn,
                DDB_INVOCATION_ID: statement_name.invocation_id,
            },
            AttributesToGet=[
                DDB_CALLBACK_DETAILS,
            ],
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE',
        )
        logger.debug({
            l_statement_name: statement_name,
            l_response: response
        })
        try:
            return CallbackSourceBuilder.get_callback_object_for_event(
                json.loads(response['Item'][DDB_CALLBACK_DETAILS])
            )
        except KeyError as ke:
            raise NoTrackedState(f"No state for {statement_name}") from ke

    @classmethod
    def get_ttl_value(cls) -> int:
        expiry_time = datetime.utcnow() + timedelta(days=ddb_ttl_in_days)
        return int(expiry_time.timestamp())

    def mark_statement_name_as_handled(self, statement_name: StatementName, finished_event_details: dict) -> None:
        """
        We take the convention that if a TTL is set the statement_name has been processed. The TTL will allow automatic
        cleanup from DDB.
        Args:
            statement_name:
            finished_event_details: information reported by Data API finished event
        """
        ttl_field = self.get_ttl_value()

        logger.debug({
            l_statement_name: str(statement_name),
            l_finished_event_details: finished_event_details,
            l_ttl: ttl_field
        })
        response = self.update_item(
            Key={
                DDB_ID: statement_name.execution_arn,
                DDB_INVOCATION_ID: statement_name.invocation_id,
            },
            UpdateExpression="SET #T = :ttl, #D = :details",
            ReturnValues='ALL_OLD',
            ReturnConsumedCapacity='NONE',
            ExpressionAttributeNames={
                '#T': DDB_TTL,
                '#D': DDB_FINISHED_EVENT_DETAILS
            },
            ExpressionAttributeValues={
                ':ttl': ttl_field,
                ':details': finished_event_details
            }
        )
        logger.debug({
            l_statement_name: statement_name,
            l_response: response,
            l_ttl: ttl_field
        })
