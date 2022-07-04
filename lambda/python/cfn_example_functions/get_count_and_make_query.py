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
CFN_OLD_RESOURCE_PROPERTIES = "OldResourceProperties"
CFN_REQUEST_TYPE = "RequestType"
CFN_REQUEST_DELETE = "Delete"
CFN_REQUEST_CREATE = "Create"
CFN_REQUEST_UPDATE = "Update"
CFN_REQUEST_TYPES = [CFN_REQUEST_CREATE, CFN_REQUEST_UPDATE, CFN_REQUEST_DELETE]
PROP_USERNAME = "username"
PROP_PASSWORD = "password"
PROP_CREATE_DB = "create_db"
PROP_CREAT_USER = "create_user"
PROP_UNRESTRICTED_SYSLOG_ACCESS = "unrestricted_syslog_access"
PROP_GROUPS = "groups"
PROP_VALID_UNTIL = "valid_until"
PROP_CONNECTION_LIMIT = "connection_limit"
PROP_SESSION_TIMEOUT = "session_timeout"
EVENT_SQL_STATEMENT = "sqlStatement"
CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA = os.environ["CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA"]

lambda_client = boto3.client('lambda')


def handler(event: dict, context):
    """
    The entry point of an execution only task is to guarantee that returned object is JSON serializable.
    """
    try:
        statementId = event['statementId']
        count = get_result(statementId)
        
        props = event["props"]
        batchSize = props['batchSize']
        whereStatement = props['whereOf']
        
        index = 0;
        queryStatments = []
        while index < count :
            offset = index * batchSize
            stmt = "select * from public.molecular_data "  + whereStatement + " order by id limit " + str(batchSize) + " offset "+  str(offset)
            queryStatments.append(stmt)
            index += batchSize;
        
        return {"statements": queryStatments}

    except Exception as ve: 
        print(ve)

def get_result(statementId):
     
    try:
        response = lambda_client.invoke(
            FunctionName=CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA,
            InvocationType='getResult',
            Payload={}
        )
    
    except Exception as ve:
        print(ve)
    
    return response.result