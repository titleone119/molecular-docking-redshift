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

EVENT_SQL_STATEMENT = "sqlStatement"
CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA = os.environ["CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA"]

lambda_client = boto3.client('lambda')

  
def handler(event, context):
    
    '''
    
        {
          "Records": [
            {
              "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
              "receiptHandle": "MessageReceiptHandle",
              "body": "json object"
            }
          ]
        }
    '''
    
    if "Records" not in event :
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid Event type')
        }
    
    records = event["Records"]
    
    print(json.dumps(records))
    
    ## check records data 
    if type(records) is not list :
        return {
            'statusCode': 400,
            'body': json.dumps('Records should be [] type')
        }
    
    if len(records) == 0 :
        return {
            'statusCode': 200,
            'body': json.dumps('nothing is done!')
        }
    
    ##make the sql statement
    sql_statement = '''insert into "public"."molecular_data"(title,smiles,format,source,category,atoms,abonds,bonds,formula,hba1,hba2,hbd, 
                        inchi,inchikey,l5,logp,mp,mr,mw,tpsa,charge,dim,energy,exactmass,file_data) values 
                    ''' 
    
    index = 0                    
    for record in records :
        
        mol_object = record
        
        ## add, before the row
        index += 1
        if index != 1 :
            sql_statement += ','
        
        sql_statement = sql_statement + '''
                        ( '{title}', '{smiles}', '{format}', '{source}', '{category}', 
                        '{atoms}', '{abonds}', '{bonds}', '{formula}', '{HBA1}',
                        '{HBA2}', '{HBD}', '{InChI}', '{InChIKey}' ,'{L5}',
                        '{logP}','{MP}','{MR}','{MW}','{TPSA}','{charge}',
                        '{dim}', '{energy}','{exactmass}','{file_data}' )
                        '''
                        
        sql_statement = sql_statement.format(**mol_object)
        
    try:             
        ##event["Records"] = {} ## set null
        event[EVENT_SQL_STATEMENT] = sql_statement
        
        logger.info(sql_statement)
    
        response = lambda_client.invoke(
            FunctionName=CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA,
            InvocationType='RequestResponse',
            Payload=json.dumps(event).encode('utf-8')
        )
        logger.info(f"Lambda returned {response}")

    except Exception as ve:
        fail_reason = f"Encountered issue {ve}"
        
        logger.error(fail_reason)
        cfnresponse.send(event, cfnresponse.FAILED, 'n/a', fail_reason)
        assert False, fail_reason
