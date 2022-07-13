
from utils import has_value
from logger import logger
import json

from datetime import datetime

  
def handler(event, context):
    '''
    molecularId: xx //means select one data
    otherwise for id list
    
    '''
    event = event['params']
    logger.info(json.dumps(event))
    
    pageSize = 10000
    if has_value(event,"pageSize"):
        pageSize = event['pageSize']
        
    index = 0
    if 'index' in event:
        index = event['index']
        logger.info("Input index: " + str(index))
    else:
        logger.info('no key: index' )
    
    idSqlStatement = 'select id from public.molecular_data where 1=1 '
    
    sqlStatement = ''
    if has_value(event,'sqlStatement'):
        sqlStatement = event['sqlStatement']
        idSqlStatement = sqlStatement
    
    rows = 0
    if has_value(event, 'rows'):
        rows = event['rows']
    
    totalCount = 0
    if has_value(event, 'totalCount'):
        totalCount = event['totalCount']
    logger.info('Input totalCount: ' + str(totalCount))
    
    totalCount += rows
    logger.info("Output totalCount: " + str(totalCount))

    idSqlStatement += ' order by id limit ' + str(pageSize) + ' offset ' + str(int(totalCount))
    
    executionId = ''
    docking_result_sql = None
    if not has_value(event, 'docking_result_sql'):
        if has_value(event,'executionId'):
            executionId = event[executionId]
        else:
            now = datetime.now()
            executionId = 'EXP-' + now.strftime("%Y%m%d-%H%M%S")
        docking_result_sql = "select count(*) from exp_data where executionid='" + executionId + "'"
    else:
        docking_result_sql = event['docking_result_sql']
    
    
    logger.info('docking_result_sql:' + docking_result_sql)
    
    response = {}
    response['index'] = index +1
    response['sqlStatement'] = sqlStatement
    response['idSqlStatement'] = idSqlStatement
    response['totalCount'] = totalCount
    response['docking_result_sql'] = docking_result_sql
    response['executionId'] = executionId
    
    return response