import requests
import boto3 as boto
import csv
import json
import pandas as pd
from io import StringIO
import io
import os.path

s3 = boto.client('s3')
s3_r = boto.resource('s3')
bucket = s3_r.Bucket(s3_bucket_kg)
ec2_user = ''
ec2_pass = ''

########MIGRATE_INTO CREDENTIALS#######################################
#Download file from S3 to local directory
for key in bucket.objects.all():
  if 'migrate_instance_creds' in key.key:
    s3dir_s3file = key.key
    s3file = s3dir_s3file
    localdir_s3file = '/tmp/' + s3file
    s3.download_file(s3_bucket_kg, s3dir_s3file, localdir_s3file)
    print('File: '+s3file+ ' saved to: '+ localdir_s3file)

with open('/tmp/migrate_instance_creds.csv') as csv_file:
  csv_reader = csv.reader(csv_file, delimiter = ',')
  line_count = 0
  for row in csv_reader:
    if line_count == 0:
      ec2_user = str(row)[2:][:-2]
      line_count += 1
    if line_count == 1:
      ec2_pass = str(row)[2:][:-2]                     
#  print('EC2 Credentials saved')
      
#print('EC2 User: ' + str(ec2_user))
#print('EC2 Password: ' + str(ec2_pass))

#######IMPORT########################################
#2 steps - 1) download projectexport.json from mtln-kareyg/python-boto/ to local /tmp/ directory and 2) import from /tmp/ directory into karey_SF_migrate 

#IMPORT PROJECT FROM S3
for key in bucket.objects.all():
  if 'projectexport' in key.key:
	#title files for boto.download_file command
    s3dir_s3file = key.key #python-boto/projectexport.json
    s3file = os.path.basename(s3dir_s3file) #projectexport.json
    localdir_s3file = '/tmp/'+s3file #/tmp/projectexport.json
    #print(s3dir_s3file, s3file, localdir_s3file) #validation

	#download file to local directory
    s3.download_file(s3_bucket_kg, s3dir_s3file, localdir_s3file)
    print('File: '+s3file+ ' saved to: '+ localdir_s3file) #validation

#INSTANCE karey_SF_migrate IMPORT PROJECT    
with open(localdir_s3file) as project_json:
  loaded=json.load(project_json)
  headers = {'Content-type': 'application/json'}
  print(loaded)
  r = requests.post('https://34.245.96.217/rest/v1/group/name/Matillion/project/import?onConflict=OVERWRITE', 
                 auth=(ec2_user,ec2_pass), 
                 verify=False,
                 json = loaded,
                 headers=headers
                  )
  print(r.status_code)
  print(r.text)
