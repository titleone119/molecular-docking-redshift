import json
import boto3
import os
import subprocess
import zlib
import binascii
import time

from logger import logger

s3 = boto3.resource('s3')

redshift_data_api = boto3.client('redshift-data')

SELECT_DATA_SQL = 'SELECT id, file_data FROM public.molecular_data where id='
INSERT_RESULT_SQL = 'INSERT into exp_result(molid,score,result_data) VALUES  '\

LIGAND_FILE_NAME = 'ligand.pdbqt'
OUT_FILE_NAME = 'vina_out.pdbqt'

RECERVER_FILE_NAME = '4EK3_rec.pdbqt'

projectPath = os.getenv("LAMBDA_TASK_ROOT")

def handler(event, context):
    
    init()
    
    for record in event['Records']:
        try:
            logger.info(json.dumps(record))
            
            msg = json.loads(record['body'])
            msg = msg[0]
            
            mol_id = msg['longValue']
            logger.info("mol id : " + str(mol_id))
            
            query_data_and_dock(mol_id)
            
        except Exception as e:
            print(e)
            raise(e)
        
        finally:
            clean_tmp_file()
            
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Hello from Lambda!')
    # }

def init():
    command = 'cp ./vina /tmp/vina; chmod 755 /tmp/vina; '
    return subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)

    #r = os.system('chmod +x ' + projectPath + '/vina')
    

def 

def query_data_and_dock(mol_id) :

    sql_stm = SELECT_DATA_SQL + str(mol_id)
    logger.info("execute_sql:" + sql_stm)
    
    response = redshift_data_api.execute_statement(
        ClusterIdentifier= os.environ["clusterIdentifier"],
        Database=os.environ["dbName"],
        DbUser=os.environ["dbUser"],
        Sql=sql_stm,
        StatementName= 'query_data_by_id_for_dock',
        # parameters=parameters,
        WithEvent=False  # When invoked from SFN with s task token we invoke using withEvent enabled.
    )
    
    logger.info("statement reponse:" + json.dumps(response,default=str))
    
    statement_id = response['Id']
    
    status = "NONE"
    while status != 'FINISHED':
        time.sleep(5/1000)
        
        response = redshift_data_api.describe_statement(Id=statement_id)
        logger.info("describe reponse:" + json.dumps(response,default=str))
    
        status = response['Status']
    
    response = redshift_data_api.get_statement_result(Id=statement_id)
    
    logger.info("getStatementResult reponse:" + json.dumps(response,default=str))

    file_data = response["Records"][0][1]["stringValue"]
    
    file_data = binascii.unhexlify(file_data)
    #file_data = zlib.decompress(tmp_data)
    logger.info('decompress: ' + file_data)
    
    ligand_file = open("ligand.pdbqt", "w")
    ligand_file.write(file_data)
    ligand_file.close()
    
    #start to dock
    dock_mol(mol_id)
    
    
def dock_mol(mol_id):
    
    vina = [projectPath + "/vina","--config" , projectPath + "/test.conf","--receptor" ,\
            projectPath +"/4EK3_rec.pdbqt" ,"--ligand", projectPath + "/1iep_ligand.pdbqt", \
            "--out", projectPath+"/vina_out.pdbqt"]
    
    subprocess.check_call(vina)
    
    report_result(mol_id)
    

def report_result(mol_id):
    
    #open text file in read mode
    text_file = open("./vina_out.pdbqt", "r")
    #read whole file to a string
    data = text_file.read()\
    #close file
    text_file.close()
 
    
    sql_stm = INSERT_RESULT_SQL + '' + str(mol_id) + ',' + data;
    
    response = redshift_data_api.execute_statement(
            ClusterIdentifier= os.environ["clusterIdentifier"],
            Database=os.environ["dbName"],
            DbUser=os.environ["dbUser"],
            Sql=sql_stm,
            StatementName='insert-mol',
            # parameters=parameters,
            WithEvent=False  # When invoked from SFN with s task token we invoke using withEvent enabled.
        )
        
def clean_tmp_file():
    
    # with open(LIGAND_FILE_NAME,'w') as ligand_file:
    #     ligand_file.remove()
    
    # with open(OUT_FILE_NAME,'w') as out_file:
    #     out_file.remove()
    
    return  
        