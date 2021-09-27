# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-lambda-function-code-cfnresponsemodule.html
# We call this cfnresponse rather than cfn_response to match more closely with the original linked above.
# We will however not depend on context as the Lambda function is not the logging that we want to point users to.
import urllib3
import json
from logger import logger

SUCCESS = "SUCCESS"
FAILED = "FAILED"

http = urllib3.PoolManager()


def send(event, response_status, physical_resource_id, reason, response_data=None, no_echo=False):
    response_url = event['ResponseURL']
    response_data = response_data or {}

    logger.debug(response_url)

    response_body = {
        'Status': response_status,
        'Reason': reason,
        'PhysicalResourceId': physical_resource_id,
        'StackId': event['StackId'],
        'RequestId': event['RequestId'],
        'LogicalResourceId': event['LogicalResourceId'],
        'NoEcho': no_echo,
        'Data': response_data
    }

    json_response_body = json.dumps(response_body)

    logger.debug(f"Response body: \n{json_response_body}")

    headers = {
        'content-type': '',
        'content-length': str(len(json_response_body))
    }

    try:
        response = http.request('PUT', response_url, headers=headers, body=json_response_body)
        logger.info(f'{{"response_body": {{{json_response_body}}}, "status_code": {response.status}}}')
        return response
    except Exception as e:
        logger.info("send(..) failed executing http.request(..):", e)
