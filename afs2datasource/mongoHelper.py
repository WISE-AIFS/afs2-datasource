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
    self._connection = None

  def connect(self, username, password, host, port, database):
    uri = 'mongodb://{username}:{password}@{host}:{port}/{database}'.format(username=username, password=password, host=host, port=port, database=database)
    connection = MongoClient(uri, serverSelectionTimeoutMS=const.DB_CONNECTION_TIMEOUT)
    connection.server_info()
    db = connection[database]
    if self._collection not in db.list_collection_names():
      raise AttributeError('collection: {} is not exist in database'.format(self._collection))
    self._connection = db[self._collection]
  
  def execute_query(self, querySql):
    data = list(self._connection.find(querySql, {'_id': 0}))
    data = pd.DataFrame(data=data)
    return data

  def check_query(self, querySql):
    try:
      if type(querySql) is str:
        querySql = json.loads(querySql)
    except:
      raise ValueError('querySql is invalid')
    return querySql