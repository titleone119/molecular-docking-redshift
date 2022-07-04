# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


import traceback
from aws_lambda_powertools.utilities.batch import sqs_batch_processor

from callback_sources.builder import CallbackSourceBuilder
from callback_sources.helper import CallbackInterface, NoCallback
from ddb.ddb_state_table import DDBStateTable
from exceptions import ConcurrentExecution, InvalidRequest
from integration import sanitize_response
from logger import logger, l_sanitized_response, l_response, l_record, l_traceback, l_exception, \
    l_callback_object
from environment_labels import env_variable_labels
from event_labels import (
    EXECUTION_ARN, SQL_STATEMENT, STATEMENT_ID, ACTION, DESCRIBE_STATEMENT, GET_STATEMENT_RESULT,
    NEXT_TOKEN, CANCEL_STATEMENT, EXECUTE_SINGLETON_STATEMENT, EXECUTE_STATEMENT
)
from assertion import assert_env_set
from redshift_data.api import describe_statement, \
    get_statement_result, cancel_statement, get_statement_id_for_statement_name, execute_statement, \
    is_statement_in_active_state
from redshift_data.finished_event import FinishedEvent
from statement_class import StatementName

for env_variable_label in env_variable_labels:
    assert_env_set(env_variable_label)

ddb_sfn_state_table = DDBStateTable()


def handler(event: dict, context):
    """
    The entry point of an execution only task is to guarantee that returned object is JSON serializable.
    """
    sanitized_response = sanitize_response(_handler(event, context))
    logger.debug({l_sanitized_response: sanitized_response})
    return sanitized_response


def get_statement_id(event: dict) -> str:
    """
    For statementId we support a placeholder VALUE 'LATEST' which will resolve the id of the latest statement issued
    form the statemachine with executionArn.
    """
    provided_statement_id = event[STATEMENT_ID]
    if provided_statement_id == 'LATEST':
        assert EXECUTION_ARN in event, f"The field {EXECUTION_ARN} is mandatory for {STATEMENT_ID}='LATEST'!"
        statement_name = ddb_sfn_state_table.get_latest_statement_name_for_execution_arn(event[EXECUTION_ARN])
        return get_statement_id_for_statement_name(str(statement_name))
    else:
        return provided_statement_id


def _handler(event: dict, context):
    logger.structure_logs(append=True, function="pre_routing")
    logger.debug(event)
    if "Records" in event:
        logger.structure_logs(append=True, function="complete_statement")
        # This event is an SQS record so this is a finished Redshift Data API event
        return sqs_finished_data_api_request_handler(event, context)
    elif SQL_STATEMENT in event:
        logger.structure_logs(append=True, function="execute_statement")
        return handle_redshift_statement_invocation_event(event)
    elif STATEMENT_ID in event and ACTION in event and event[ACTION] == DESCRIBE_STATEMENT:
        logger.structure_logs(append=True, function="describe_statement")
        return describe_statement(get_statement_id(event))
    elif STATEMENT_ID in event and ACTION in event and event[ACTION] == GET_STATEMENT_RESULT:
        logger.structure_logs(append=True, function="get_statement_result")
        return get_statement_result(get_statement_id(event), next_token=event.get(NEXT_TOKEN))
    elif STATEMENT_ID in event and ACTION in event and event[ACTION] == CANCEL_STATEMENT:
        logger.structure_logs(append=True, function="cancel_statement")
        return cancel_statement(get_statement_id(event))
    else:
        raise InvalidRequest(f"Unsupported invocation event {event}.")


def handle_redshift_statement_invocation_event(event):
    assert SQL_STATEMENT in event, f"Programming error should never handle invocation without SQL_STATEMENT {event}."
    logger.info(event)
    sql_statement = event[SQL_STATEMENT]
    action = event.get(ACTION)
    
    if action == EXECUTE_SINGLETON_STATEMENT or action == EXECUTE_STATEMENT or action is None:
        run_as_singleton = action == EXECUTE_SINGLETON_STATEMENT
        callback_object = CallbackSourceBuilder.get_callback_object_for_event(event)
        
        parameters = None
        if "Parameters" in event:
            parameters = event.get("Parameters")
            
        return handle_redshift_statement_invocation(sql_statement, callback_object, run_as_singleton, parameters)
    else:
        raise InvalidRequest(f"Unsupported {ACTION} to execute sql_statement {event}")


def handle_redshift_statement_invocation(sql_statement: str, callback_object: CallbackInterface, run_as_singleton=False, parameters = None):
    
    
    if run_as_singleton and is_statement_in_active_state(sql_statement):
        raise ConcurrentExecution(f"There is already an instance of {sql_statement} running.")
    statement_name = ddb_sfn_state_table.register_execution_start(callback_object, sql_statement)
    with_event = not isinstance(callback_object, NoCallback)
    if not with_event:
        logger.debug(f'No callback for {sql_statement}')
    response = execute_statement(sql_statement, str(statement_name), with_event=with_event, parameters=parameters)
    logger.info({
        l_response: response,
        l_callback_object: callback_object
    })
    return response


def finished_data_api_request_record_handler(record: dict):
    """
    This will be called for each finished invocation.
    It should raise an exception if the message was not processed successfully so we don't catch any exceptions
    and if we would we should be able to handle it or re-raise.

    Args:
        record: Has 'body' as json string of event documented in section ata-api-monitoring-events-finished on
                https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-monitoring-events.html

    Returns:
        None:
    """
    try:
        logger.debug(record)
        finished_event = FinishedEvent.from_record(record)
        statement_name = StatementName.from_str(finished_event.get_statement_name())
        callback_source = ddb_sfn_state_table.get_callback_source_for_statement_name(statement_name)
        if finished_event.has_failed():
            # noinspection PyBroadException
            try:
                statement_description = describe_statement(finished_event.get_statement_id())
                error = statement_description["Error"]
                finished_event['detail']['error'] = error
            except Exception as ex:
                logger.warn(f"Could not get error for {finished_event} due to {ex}")
            callback_source.send_failure(statement_name, finished_event)
        elif finished_event.has_succeeded():
            callback_source.send_success(statement_name, finished_event)
        else:
            raise NotImplementedError(f"Unsupported Data API finished event state {finished_event.get_state()}")

        ddb_sfn_state_table.mark_statement_name_as_handled(statement_name, finished_event)
    except Exception as e:
        logger.fatal({
            l_record: record,
            l_exception: e,
            l_traceback: traceback.format_exc()
        })
        raise e


@sqs_batch_processor(record_handler=finished_data_api_request_record_handler)
def sqs_finished_data_api_request_handler(event, context):
    logger.debug({"event": event, "context": context})
    return {"statusCode": 200}
