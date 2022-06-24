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
        
        
def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    #s3://open-base/Unsaved/2022/02/25/64931b05-9f92-464c-a68e-85865ddd9352.csv
    #s3://open-base/Unsaved/2022/02/25/c68ed9e8-4c01-48e3-9a63-94d80e28b90c.csv
    #s3://open-base/Unsaved/2022/02/25/d2d4aaef-77d1-4e1d-bb37-9f391ef0b426.csv
    #s3://open-base/Unsaved/2022/02/25/d2d4aaef-77d1-4e1d-bb37-9f391ef0b426.csv.metadata
    
    # Get the object from the event and show its content type
    #bucket = event['Records'][0]['s3']['bucket']['name']
    #key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    print(json.dumps(event['Records']))
    records = event["Records"]
    
    if type(records) is not list :
        return {
            'statusCode': 400,
            'body': json.dumps('Records should be [] type')
        }
    
    
    event_source = event[""];
    
            
    for record in records]:
        
        print("start one recored")
        event_source = record["eventSource"]
        bucket_name = ''
        object_key = ''
        
       if "aws:s3" == event_source:
                bucket_name,object_key = parse_S3_notification_record(record)
                
       if "aws:sqs" == event[] 
   
        else
            bucket_name,object_key = parse_SQS_record(record)
             
        try: 
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            
            print("Info download object: " + object_key + ", response size:" + str(response['ContentLength']) )
        
        except Exception as e:
            raise e
        
   
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
