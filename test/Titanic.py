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

import os
import json
import pandas as pd
from afs2datasource import DBManager, constant

with open('config/azureblob.json') as f:
  data = json.load(f)
  os.environ = data

db = DBManager()
db.connect()

if db.get_dbtype() == constant.DB_TYPE['S3'] or db.get_dbtype() == constant.DB_TYPE['AZUREBLOB']:
  bucket_name = 'titanic'
  file_name = 'titanic.csv'
  if not db.is_table_exist(bucket_name):
    db.create_table(bucket_name)
  db.insert(table_name=bucket_name, source='test/titanic.csv', destination=file_name)
else:
  df = pd.read_csv('test/titanic.csv')
  # create table
  columns = [
    {'name': 'passenger_id', 'type': 'INTEGER'},
    {'name': 'survived', 'type': 'INTEGER'},
    {'name': 'pclass', 'type': 'INTEGER'},
    {'name': 'name', 'type': 'TEXT'},
    {'name': 'sex', 'type': 'TEXT'},
    {'name': 'age', 'type': 'INTEGER'},
    {'name': 'sib_sp', 'type': 'INTEGER'},
    {'name': 'parch', 'type': 'INTEGER'},
    {'name': 'ticket', 'type': 'TEXT'},
    {'name': 'fare', 'type': 'FLOAT'},
    {'name': 'cabin', 'type': 'TEXT'},
    {'name': 'embarked', 'type': 'TEXT'}
  ]

  table_name = ''
  if db.get_dbtype() == constant.DB_TYPE['POSTGRES']:
    table_name = 'afs.titanic'
  elif db.get_dbtype() == constant.DB_TYPE['MONGODB']:
    table_name = 'titanic'
  elif db.get_dbtype() == constant.DB_TYPE['INFLUXDB']:
    table_name = 'titanic'

  if not db.is_table_exist(table_name):  
    db.create_table(table_name, columns)
  # insert records
  db.insert(table_name, list(df.columns.values), df.values)

  # close db
  db.disconnect()