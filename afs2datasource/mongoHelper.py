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
import afs2datasource.constant as const
import afs2datasource.utils as utils
from pymongo import MongoClient, errors
import pandas as pd

class MongoHelper():
  def __init__(self):
    self._db = ''
    self._collection = ''
    self._connection = None

  def connect(self):
    if self._connection is None:
      data = utils.get_data_from_dataDir()
      username, password, host, port, database = utils.get_credential_from_dataDir(data)
      uri = 'mongodb://{}:{}@{}:{}/{}'.format(username, password, host, port, database)
      self._connection = MongoClient(uri)
      self._connection.server_info()
      self._db = database
      self._collection = data.get('collection', '')
  
  def disconnect(self):
    if self._connection:
      self._connection.close()
      self._collection = ''
      self._connection = None
      self._db = ''
      self._collection = ''

  async def execute_query(self, querySql):
    if not self._collection:
      raise AttributeError('No collection in data')
    data = list(self._connection[self._db][self._collection].find(querySql, {'_id': 0}))
    data = pd.DataFrame(data=data)
    return data

  def check_query(self, querySql):
    try:
      if type(querySql) is str:
        querySql = json.loads(querySql)
    except:
      raise ValueError('querySql is invalid')
    return querySql

  def is_table_exist(self, table_name):
    return table_name in self._connection[self._db].list_collection_names()

  def create_table(self, table_name, columns):
    self._connection[self._db].create_collection(table_name)

  def insert(self, table_name, columns, records):
    records = list(map(lambda record: dict(zip(columns, record)), records))
    result = self._connection[self._db][table_name].insert_many(records)