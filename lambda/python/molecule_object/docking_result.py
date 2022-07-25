# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# This is just meant as a sample function that can show how to orchestrate tasks on Redshift via Cloudformation.
# This is by no means production ready code.

import json
import os
from logger import logger
import binascii

import psycopg2

  
def handler(event, context):
    
    '''
        {
          "Records": ["body":
            {
              "molId": 1233,
              "executionId": "EXP-XX",
              "score": 0,
              "data":"bytesxbacjfajsidfa"
            }
          ]
        }
    '''
    logger.info("Records Count: " + str(len(event["Records"])))
    
    
    sql_stm = "INSERT into exp_data(molid,executionid,score,result) VALUES " 
    
    index = 0
    for record in event['Records']:
        record = json.loads(record['body'])
        molId = record['molId']
        executionId = record['executionId']
        score = record['score']
        data = record['data']
        data = data.replace("'"," ")
        data = data.replace('"',' ')
        
        logger.info("sub of data:" + data[0:100])
        
        #score = len(data)
        
        if index != 0 :
            sql_stm += ","
            
        sql_stm += " (" + str(molId) + ", '" + executionId + "'," + str(score) + ",'" + data + "')";
        index +=1
        
        
    #Execute SQL Command on Redshift Server
    execute_sql(sql_stm)
    return {}
    
    

def execute_sql(sql_stm, parameters = None):
    
    
    conn_string = "dbname='dev' port='5439' user='rsadmin' password='ABCDefg1234!!' host='10.0.0.41'"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    try: 
        logger.info("execute_sql:" + sql_stm[0:200])
        
        cursor = conn.cursor()
        result = cursor.execute(sql_stm);
        
        cursor.close()
        
    except Exception as ve:
        fail_reason = f"Encountered issue in executing sql:   {ve}"
        logger.error(fail_reason)
        raise ve
    finally:
        conn.close()
        
    logger.info("SQL Done!")
    
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