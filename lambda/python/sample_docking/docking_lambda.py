import json
import boto3
import os
import subprocess
s3 = boto3.resource('s3')

BUCKET_NAME = 'aws-serverless-molecular-docking'
VINA_NAME = 'vina_1.2.3_linux_x86_64'

CONFIG_NAME = 'test.conf'

RECEPTOR_NAME = '1iep_receptor.pdbqt'
LIGAND_NAME = '1iep_ligand.pdbqt'

MINIMIZED_NAME = '1iep_ligand_minimized.pdbqt'
VINA_OUT_NAME = '1iep_ligand_out.pdbqt'

def lambda_handler(event, context):
    
    # Download vina
    s3.Object(BUCKET_NAME, VINA_NAME).download_file('/tmp/vina')
    r = os.system('chmod +x /tmp/vina')
    
    # # Download config
    # s3.Object(BUCKET_NAME, CONFIG_NAME).download_file('/tmp/test.conf')    
    
    # Download files
    s3.Object(BUCKET_NAME, 'input/'+RECEPTOR_NAME).download_file('/tmp/'+RECEPTOR_NAME)
    s3.Object(BUCKET_NAME, 'input/'+LIGAND_NAME).download_file('/tmp/'+LIGAND_NAME)
    
    vina = ["/tmp/vina","--config" ,"/mnt/serverless-molecular-docking/test.conf","--receptor" ,"/tmp/1iep_receptor.pdbqt" ,"--ligand","/tmp/1iep_ligand.pdbqt"]
    
    subprocess.check_call(vina)
    
    # s3.Object(BUCKET_NAME, 'output/'+MINIMIZED_NAME).upload_file('/tmp/'+MINIMIZED_NAME)
    s3.Object(BUCKET_NAME, 'output/'+VINA_OUT_NAME).upload_file('/tmp/'+VINA_OUT_NAME)
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Hello from Lambda!')
    # }