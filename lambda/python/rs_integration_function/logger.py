# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0


from aws_lambda_powertools import Logger

# Labels used as log keys, alphabetically
l_callback_object = 'callback_object'
l_default = 'default'
l_exception = 'exception'
l_finished_event_details = 'finished_event_details'
l_id = 'id'
l_item = 'item'
l_message = 'message'
l_next_token = 'next_token'
l_record = 'record'
l_response = 'response'
l_rs_integration_function = 'rs_integration_function'
l_sanitized_response = 'sanitized_response'
l_statement_name = 'statement_name'
l_task_timed_out = 'task_timed_out'
l_task_token = 'task_token'
l_traceback = 'traceback'
l_ttl = 'ttl'

logger = Logger()
