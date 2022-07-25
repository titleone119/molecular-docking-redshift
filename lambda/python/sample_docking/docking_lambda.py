import json
import boto3
import os
import subprocess
import zlib
import binascii
import time

import psycopg2

from logger import logger

s3 = boto3.resource('s3')

SELECT_DATA_SQL = 'SELECT id, file_data::varchar FROM public.molecular_data where id='
INSERT_RESULT_SQL = "INSERT into exp_data(molid,executionid,score,result_data) VALUES "

LIGAND_FILE_NAME = 'ligand.pdbqt'
OUT_FILE_NAME = 'vina_out.pdbqt'

RECERVER_FILE_NAME = '4EK3_rec.pdbqt'

projectPath = os.getenv("LAMBDA_TASK_ROOT")
# Create SQS client
sqs = boto3.client('sqs')

global conn

def handler(event, context):
    
    init()
    
    for record in event['Records']:
        try:
            #logger.info(json.dumps(record))
            
            msg = json.loads(record['body'])
            # msg = msg[0]
            
            mol_id = msg["molId"]
            execution_id = msg["executionId"]
            
            logger.info("mol id : " + str(mol_id) + ", execution_id" + execution_id)
            
            query_data_and_dock(mol_id,execution_id)
            
            
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
    command = 'rm -rf /tmp/*; cp ' + projectPath + '/vina /tmp/vina; chmod 755 /tmp/vina; '  
                # + projectPath + '/test.conf /tmp/test.conf; chmod 644 /tmp/test.conf; cp ' \
                # + projectPath + '/4EK3_rec.pdbqt /tmp/4EK3_rec.pdbqt; chmod 644 /tmp/4EK3_rec.pdbqt; ' 
                # + projectPath + '/ligand.pdbqt /tmp/ligand.pdbqt; chmod 644 /tmp/ligand.pdbqt; ' 
                
    subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
    
    global queue_url
    queue_url = os.environ['docking_result_queue']

def query_data_and_dock(mol_id,execution_id) :
    
    
    conn_string = "dbname='dev' port='5439' user='rsadmin' password='ABCDefg1234!!' host='10.0.0.41'"
    global conn
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    #r = os.system('chmod +x ' + projectPath + '/vina')
    
    # conn_string = "dbname='dev' port='5439' user='rsadmin' password='ABCDefg1234!!' host='10.0.0.41'"
    # conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("select id, file_data from public.molecular_data where id = '" + str(mol_id) + "'")
    #for record in cursor:
    record = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    
    if record is not None:
        try:
            #logger.info(record)
            file_data = record[1]
            
            file_data = binascii.unhexlify(file_data)
            file_data = binascii.unhexlify(file_data)
            
            #logger.info('unhexlify: ' + file_data)
            file_data = zlib.decompress(file_data)
            file_data = file_data.decode('utf-8')
            
            file_data = file_data.replace("'"," ")
            #logger.info('decompress: ' + str(file_data))
            
            ligand_file = open("/tmp/ligand.pdbqt", "w")
            ligand_file.write(file_data)
            
            ligand_file.close()
            
            #start to dock
            dock_mol(mol_id,execution_id)
        except Exception as ve:
            fail_reason = f"Encountered issue in docking:   {ve}"
            logger.error(ve)
            msg = {
                "molId":mol_id,
                "executionId": execution_id,
                "score": 0,
                "data": ""
            }
            report_result(msg)
    
    
def dock_mol(mol_id,execution_id):
    
    vina = [ "/tmp/vina","--config",  projectPath + "/test.conf","--receptor",\
            projectPath + "/4EK3_rec.pdbqt", "--ligand", "/tmp/ligand.pdbqt", \
            "--out", "/tmp/vina_out.pdbqt"]
    subprocess.call(vina,timeout=800)
    # vina = "/tmp/vina --config /tmp/test.conf --receptor /tmp/4EK3_rec.pdbqt --ligand /tmp/1iep_ligand.pdbqt --out /tmp/vina_out.pdbqt"
    # output = subprocess.check_output(vina, shell=True)
    # logger.info(output)
    
    #open text file in read mode
    text_file = open("/tmp/vina_out.pdbqt", "r")
    #read whole file to a string
    lines = text_file.readlines()
    
    #close file
    text_file.close()
    
    line = lines[1] #2nd line
    score = float(line.split(':')[1].split()[0])
    
    out_data = "\n".join(lines)# Concatenate all lines
    
    #close file
    text_file.close()
    
    msg = {
        "molId":mol_id,
        "executionId": execution_id,
        "score": score,
        "data":out_data
    }
    report_result(msg)

def report_result(msg):
    
    msg_str = json.dumps(msg)
    global queue_url
    sqs.send_message(
        QueueUrl=queue_url,
        MessageAttributes={},
        MessageBody=msg_str
    )
    logger.info('sent msg: '  +  msg_str[0:100] )
        
def clean_tmp_file():
    
    # with open(LIGAND_FILE_NAME,'w') as ligand_file:
    #     ligand_file.remove()
    
    # with open(OUT_FILE_NAME,'w') as out_file:
    #     out_file.remove()
    
    return  
        