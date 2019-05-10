import os
import json
import afs2datasource.constant as const
from pymongo import MongoClient, errors
import pandas as pd

class MongoHelper():
  def __init__(self):
    dataDir = os.getenv('PAI_DATA_DIR', {})
    if type(dataDir) is str:
      dataDir = json.loads(dataDir)
    data = dataDir.get('data', {})
    collection = data.get('collection', None)
    if not collection:
      raise AttributeError('No collection in data')
    self._collection = collection
    self._db = ''
    self._connection = None

  def connect(self, username, password, host, port, database):
    uri = 'mongodb://{username}:{password}@{host}:{port}/{database}'.format(username=username, password=password, host=host, port=port, database=database)
    if self._connection is None:
      self._connection = MongoClient(uri, serverSelectionTimeoutMS=const.DB_CONNECTION_TIMEOUT)
      self._connection.server_info()
      self._db = database
  
  def disconnect(self):
    if self._connection:
      self._connection.close()
      self._connection = None
      self._db = ''

  def execute_query(self, querySql):
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
    print(result.inserted_ids)