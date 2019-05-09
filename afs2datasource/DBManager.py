import os
import json
import afs2datasource.utils as utils
import afs2datasource.constant as const
import afs2datasource.mongoHelper as mongoHelper
import afs2datasource.postgresHelper as postgresHelper
import afs2datasource.influxHelper as influxHelper

class DBManager:
  def __init__(self):
    dataDir = os.getenv('PAI_DATA_DIR', None)
    if not dataDir:
      raise AttributeError('No dataDir')
    if type(dataDir) is str:
      dataDir = json.loads(dataDir)
    dbType = dataDir.get('type', None)
    if dbType is None:
      raise AttributeError('No type in dataDir')
    if dbType not in const.DB_TYPE.values():
      raise ValueError('{0} is not support'.format(dbType))
    data = dataDir.get('data', {})
    querySql = data.get('querySql', None)
    if not querySql and not(type(querySql) is dict):
      raise AttributeError('No querySql in dataDir[data]')
    self._status = const.DB_STATUS['DISCONNECTED']
    self._username, self._password, self._host, self._port, self._database = utils.get_credential(data)
    self._helper = self._create_helper(dbType)
    self._querySql = querySql

  def _create_helper(self, dbType):
    dbType = dbType.lower()
    if dbType == const.DB_TYPE['MONGODB']:
      return mongoHelper.MongoHelper()
    elif dbType == const.DB_TYPE['POSTGRES']:
      return postgresHelper.PostgresHelper()
    elif dbType == const.DB_TYPE['INFLUXDB']:
      return influxHelper.InfluxHelper()
    else:
      return None

  def connect(self):
    try:
      self._status = const.DB_STATUS['CONNECTING']
      self._helper.connect(self._username, self._password, self._host, self._port, self._database)
      self._status = const.DB_STATUS['CONNECTED']
    except Exception as ex:
      self._status = const.DB_STATUS['DISCONNECTED']
      raise ex

  def is_connected(self):
    return self._status == const.DB_STATUS['CONNECTED']
  
  def execute_query(self):
    self._querySql = self._helper.check_query(self._querySql)
    return self._helper.execute_query(self._querySql)