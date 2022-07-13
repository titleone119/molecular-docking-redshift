# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# This is just meant as a sample function that can show how to orchestrate tasks on Redshift via Cloudformation.
# This is by no means production ready code.

import json
import math
import os
import boto3
from logger import logger

CFN_REQUEST_TYPE = "RequestType"
CFN_REQUEST_DELETE = "Delete"
CFN_REQUEST_CREATE = "Create"
CFN_REQUEST_UPDATE = "Update"

EVENT_SQL_STATEMENT = "sqlStatement"
CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA = os.environ["CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA"]

lambda_client = boto3.client('lambda')

redshift_data_api = boto3.client('redshift-data')
  
def handler(event, context):
    
    '''
        {
          "operation" : 'insert,delete,'
          "Records": [
            {
              "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
              "receiptHandle": "MessageReceiptHandle",
              "body": "json object"
            }
          ]
        }
    '''
    logger.info(event)
    print('event......')
    event = json.loads(event['body']) 
    print(json.dumps(event))
    
    if not isinstance(event,dict):
        event = json.loads(event)
    
    logger.info("event:" + json.dumps(event))
    
    sql_stm = None
    
    if 'operation' in event and event['operation'] == 'insert':
        
        if "Records" not in event :
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid Event type')
            }
        
        records = event["Records"]
        if len(records) == 0 :
            return {
                'statusCode': 200,
                'body': json.dumps('nothing is done!')
            }
        sql_stm = do_insert(records)
        
    else:
       if 'sql_statement' in event:
           sql_stm = event['sql_statement']
           
    if sql_stm is None:
        return {
            'statusCode': 400,
            'body': operation + ' is invalid. Nothing is done!'
        }
        
    #Execute SQL Command on Redshift Server
    response = execute_sql(sql_stm)
    return response
    
    
def do_insert(records : list):
    
    ##make the sql statement
    sql_statement = '''insert into "public"."molecular_data"(title,smiles,format,source,category,atoms,abonds,bonds,formula,hba1,hba2,hbd, inchi,inchikey,l5,logp,mp,mr,mw,tpsa,charge,dim,energy,exactmass,file_data) values 
                    ''' 
    
    index = 0                    
    for record in records :
        
        mol_object = make_defaults(record)
        
        ## add, before the row
        index += 1
        if index != 1 :
            sql_statement += ','
        
        sql_statement = sql_statement + '''
                        ( '{title}', '{smiles}', '{format}', '{source}', '{category}', 
                        {atoms}, {abonds}, {bonds}, '{formula}', {HBA1},
                        {HBA2}, {HBD}, '{InChI}', '{InChIKey}' ,{L5},
                        {logP},{MP},{MR},{MW},{TPSA},{charge},
                        '{dim}', {energy},{exactmass},'{file_data}' )
                        '''
                        
        sql_statement = sql_statement.format(**mol_object)
    logger.info("sql: " + sql_statement)
    return sql_statement
    

def execute_sql(sql_stm, parameters = None):
    try: 
        logger.info("execute_sql:" + sql_stm)
    
        # response = lambda_client.invoke(
        #     FunctionName=CDK_STEPFUNCTIONS_REDSHIFT_LAMBDA,
        #     InvocationType='RequestResponse',
        #     Payload=json.dumps(event).encode('utf-8')
        # )
        
        response = redshift_data_api.execute_statement(
            ClusterIdentifier= os.environ["clusterIdentifier"],
            Database=os.environ["dbName"],
            DbUser=os.environ["dbUser"],
            Sql=sql_stm,
            StatementName='insert-mol',
            # parameters=parameters,
            WithEvent=False  # When invoked from SFN with s task token we invoke using withEvent enabled.
        )
        
        # Must decode the payload
        logger.info("done exuecting....")
        
        logger.info(response)
        
        lambda_response = {}
        
        lambda_response['statusCode'] = 200
        lambda_response['body'] = json.dumps(response["ResponseMetadata"])
        logger.info(f"Lambda returned {(lambda_response)}")
        
        return lambda_response
    except Exception as ve:
        fail_reason = f"Encountered issue in executing sql:   {ve}"
        logger.error(ve)
        return {
            "statusCode": 400,
            "Error":fail_reason
        }


def make_defaults(json_obj):
    new_json = json_obj.copy()
    
    logger.info('record :')
    logger.info(type(new_json))
    
    ##string values
    # for key in ['title','smiles','format','source','category','formula','InChI','InChIKey','dim']:
         
    logger.info('1')
    
    if 'title' not in json_obj or isnan(json_obj['title']):
        new_json['title'] = '' 
    
         
    logger.info('2') 
    if 'smiles' not in json_obj or isnan(json_obj['smiles']):
            new_json['smiles'] = ''
            
    if 'format' not in json_obj or isnan(json_obj['format']):
        logger.info(3.33)
        new_json['format'] = 'pdbqt'
    
    logger.info('4')
    if 'source' not in json_obj or isnan(json_obj['source']):
        logger.info('source: ' + json_obj['source'])
        new_json['source'] = ' '
    
    
    logger.info('5')
    if 'category' not in json_obj or isnan(json_obj['category']):
        new_json['category'] = ' '
    
    if 'formula' not in json_obj or isnan(json_obj['formula']):
        new_json['formula'] = ''
    
    if 'InChI' not in json_obj or isnan(json_obj['InChI']):
        new_json['InChI'] = ''
    
    if 'InChIKey' not in json_obj or isnan(json_obj['InChIKey']):
        new_json['InChIKey'] = ''
        
    if 'dim' not in json_obj or isnan(json_obj['dim']):
        new_json['dim'] = '3'
        
    ## num values
    # for key in ['atoms','abonds','bonds','cansmi','cansmins', 'HBA1','HBA2','HBD', 'L5','logP','MP','MR','MW','TPSA','charge','energy','exactmass']:
    if 'atoms' not in json_obj or isnan(json_obj['atoms']):
        new_json['atoms'] = 1
        
    if 'abonds' not in json_obj or isnan(json_obj['abonds']):
        new_json['abonds'] = 0

    if 'bonds' not in json_obj or isnan(json_obj['bonds']):
        new_json['bonds'] = 0
    
    if 'cansmi' not in json_obj or isnan(json_obj['cansmi']):
        new_json['cansmi'] = 0
     
    if 'cansmins' not in json_obj or isnan(json_obj['cansmins']):
        new_json['cansmins'] = 0
        
    if 'HBA1' not in json_obj or isnan(json_obj['HBA1']):
        new_json['HBA1'] = 0
     
    if 'HBA2' not in json_obj or isnan(json_obj['HBA2']):
        new_json['HBA2'] = 0
        
    if 'HBD' not in json_obj or isnan(json_obj['HBD']):
        new_json['HBD'] = 0
        
    if 'L5' not in json_obj or isnan(json_obj['L5']):
        new_json['L5'] = 0
        
    if 'logP' not in json_obj or isnan(json_obj['logP']):
        new_json['logP'] = 0
        
    if 'MP' not in json_obj or isnan(json_obj['MP']):
        new_json['MP'] = 0
        
    if 'MR' not in json_obj or isnan(json_obj['MR']):
        new_json['MR'] = 0
        
    if 'MW' not in json_obj or isnan(json_obj['MW']):
        new_json['MW'] = 0
        
    if 'TPSA' not in json_obj or isnan(json_obj['TPSA']):
        new_json['TPSA'] = 0
        
    if 'charge' not in json_obj or isnan(json_obj['charge']):
        new_json['charge'] = 0
        
    if 'energy' not in json_obj or isnan(json_obj['energy']):
        new_json['energy'] = 0
        
    if 'exactmass' not in json_obj or isnan(json_obj['exactmass']):
        new_json['exactmass'] = 0
       
    ## bytes
    if 'file_data' not in json_obj or isnan(json_obj['file_data']):
        new_json['file_data'] = ''
    
    return new_json                   

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


class MolObject(object):
    
    def __init__(self, mol_json, connection_limit=-1,
                 session_timeout=-1):
        logger.info('init')