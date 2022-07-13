# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# This is just meant as a sample function that can show how to orchestrate tasks on Redshift via Cloudformation.
# This is by no means production ready code.

import json
import math
import os

from logger import logger


EVENT_SQL_STATEMENT = "sqlStatement"
CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA = os.environ["CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA"]

def handler(event, context):
    
    '''
        Records:[{
          long: "fdadfsasfd-dfad",
          bytearray: 'ddddddddd'
        }]
    '''
    
    
    try:
        for record in event['Records']:
            print(json.dumps(record))
        
        #insert result to redshift
        
    except Exception as ve:
        fail_reason = f"Encountered issue {ve}"
        logger.error(fail_reason)
        return {
            "statusCode": 400,
            "body":fail_reason
        }
    
    # return response

### utils
def isnan(x):
    return x != x


def has_value(as_dict,key):
    if key not in as_dict:
        return False;
    tmp = as_dict[key]
    if isnan(tmp):
        return False
    return True
