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

import re
import json
import afs2datasource.utils as utils
from afs2datasource.helper import Helper
from pymongo import MongoClient
import pandas as pd

ISODATE_PREFIX = 'afs-isotime-'
import dateutil.parser
def ISODateDecoder(dic):
  for key, value in dic.items():
    if not ISODATE_PREFIX in value:
      continue
    value = value.split(ISODATE_PREFIX)[-1].strip()
    try:
      value = dateutil.parser.isoparse(value)
    finally:
      dic[key] = value
  return dic

class MongoHelper(Helper):
  def __init__(self, dataDir):
    self._connection = None
    data = utils.get_data_from_dataDir(dataDir)
    self.username, self.password, self.host, self.port, self.database = utils.get_credential_from_dataDir(data)
    self._db = self.database
    self._collection = data.get('collection', '')    

  async def connect(self):
    if self._connection is None:
      uri = 'mongodb://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database)
      self._connection = MongoClient(uri)
      self._connection.server_info()
  
  def disconnect(self):
    if self._connection:
      self._connection.close()
      self._connection = None

  async def execute_query(self, querySql):
    if not self._collection:
      raise AttributeError('No collection in data')
    data = list(self._connection[self._db][self._collection].find(querySql, {'_id': 0}))
    data = pd.DataFrame(data=data)
    return data

  def check_query(self, querySql):
    try:
      if type(querySql) is str:
        querySql = re.compile('ISODate\("([^"]+)"\)').sub('"{}{}"'.format(ISODATE_PREFIX, '\\1'), querySql)
        querySql = json.loads(querySql, object_hook=ISODateDecoder)
    except:
      raise ValueError('querySql is invalid')
    return querySql

  def is_table_exist(self, table_name):
    return table_name in self._connection[self._db].list_collection_names()

  def is_file_exist(self, table_name, file_name):
    raise NotImplementedError('Mongodb not implement.')

  def create_table(self, table_name, columns):
    self._connection[self._db].create_collection(table_name)

  def insert(self, table_name, columns, records):
    records = list(map(lambda record: dict(zip(columns, record)), records))
    result = self._connection[self._db][table_name].insert_many(records)

  async def delete_table(self, table_name):
    self._connection[self._db][table_name].drop()

  def delete_record(self, table_name, condition):
    try:
      if type(condition) is str:
        condition = json.loads(condition)
    except:
      raise ValueError('querySql is invalid')
    self._connection[self._db][table_name].delete_many(condition)
