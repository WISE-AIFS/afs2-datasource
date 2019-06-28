# Copyright 2019 WISE-PaaS/AFS
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License. 

from afs2datasource import DBManager, constant

print('-----POSTGRES-----')
# postgres
db_type = constant.DB_TYPE['POSTGRES']
username = ''
password = ''
host = ''
port = 
database = ''
querySql = ''
collection = None

db = DBManager(db_type=db_type,
username=username,
password=password,
host=host,
port=port,
database=database,
querySql=querySql)
db.connect()
try:
  db.execute_query()
  print(True)
except Exception as e:
  print(e)

print('-----MONGODB-----')
# mongo
db_type = constant.DB_TYPE['MONGODB']
username = ''
password = ''
host = ''
port = 
database = ''
querySql = '{}'
collection = ''

db = DBManager(db_type=db_type,
username=username,
password=password,
host=host,
port=port,
database=database,
collection=collection,
querySql=querySql)
db.connect()
try:
  db.execute_query()
  print(True)
except Exception as e:
  print(e)

print('-----INFLUX-----')
# influx
db_type = constant.DB_TYPE['INFLUXDB']
username = ''
password = ''
host = ''
port = ''
database = ''
querySql = ''

db = DBManager(db_type=db_type,
username=username,
password=password,
host=host,
port=port,
database=database,
querySql=querySql)
db.connect()
try:
  db.execute_query()
  print(True)
except Exception as e:
  print(e)

print('-----S3-----')
# influx
db_type = constant.DB_TYPE['S3']
endpoint = ''
access_key = ''
secret_key = ''
bucket_name = ''
blob_list = ['']

db = DBManager(db_type=db_type,
endpoint=endpoint,
access_key=access_key,
secret_key=secret_key,
bucket_name=bucket_name,
blob_list=blob_list)
db.connect()
try:
  db.execute_query()
  print(True)
except Exception as e:
  print(e)

print('-----APM-----')
# APM
db_type = constant.DB_TYPE['APM']
username = ''
password = ''
apmUrl = ''
timeRange = []
parameterList = []
machineIdList = []
mongouri = ''

db = DBManager(db_type=db_type,
username=username,
password=password,
apmUrl=apmUrl,
machineIdList=machineIdList,
parameterList=parameterList,
mongouri=mongouri,
timeRange=timeRange)
db.connect()
try:
  db.execute_query()
  print(True)
except Exception as e:
  print(e)