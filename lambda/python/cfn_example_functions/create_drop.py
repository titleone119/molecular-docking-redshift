# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# This is just meant as a sample function that can show how to orchestrate tasks on Redshift via Cloudformation.
# This is by no means production ready code.

import json
import os
import boto3
from logger import logger
import cfnresponse


CFN_RESOURCE_PROPERTIES = "ResourceProperties"
CFN_REQUEST_TYPE = "RequestType"
CFN_REQUEST_DELETE = "Delete"
CFN_REQUEST_CREATE = "Create"
CFN_REQUEST_UPDATE = "Update"
CFN_REQUEST_TYPES = [CFN_REQUEST_CREATE, CFN_REQUEST_UPDATE, CFN_REQUEST_DELETE]
PROP_CREATE_SQL = "create_sql"  # The statement that should be ran when Cloudformation creates the resource
PROP_DROP_SQL = "drop_sql"  # The statement that should be ran when Cloudformation drops the resource
EVENT_SQL_STATEMENT = "sqlStatement"
CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA = os.environ["CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA"]

lambda_client = boto3.client('lambda')


def handler(event: dict, context):
    try:
        logger.debug(json.dumps(event))
        props = event[CFN_RESOURCE_PROPERTIES]
        for field in [PROP_CREATE_SQL, PROP_DROP_SQL]:
            assert field in props, f"{field} is a mandatory field"

        request_type = event[CFN_REQUEST_TYPE]

        if request_type == CFN_REQUEST_DELETE:
            sql_statement = props[PROP_DROP_SQL]
        elif request_type == CFN_REQUEST_CREATE:
            sql_statement = props[PROP_CREATE_SQL]
        else:
            # This component only supports constant CREATE/DELETE SQLs
            raise ValueError(f"{CFN_REQUEST_TYPE} must be one of {CFN_REQUEST_TYPES}")

        event[EVENT_SQL_STATEMENT] = sql_statement

        response = lambda_client.invoke(
            FunctionName=CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA,
            InvocationType='RequestResponse',
            Payload=json.dumps(event).encode('utf-8')
        )
        logger.info(f"Lambda returned {response}")

    except Exception as ve:
        fail_reason = f"Encountered issue {ve}"
        cfnresponse.send(event, cfnresponse.FAILED, 'n/a', fail_reason)
        assert False, fail_reason
