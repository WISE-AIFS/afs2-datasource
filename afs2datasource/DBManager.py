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
    self._dbType = dbType
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
      if self.is_connected() or self.is_connecting():
        return
      self._status = const.DB_STATUS['CONNECTING']
      self._helper.connect(self._username, self._password, self._host, self._port, self._database)
      self._status = const.DB_STATUS['CONNECTED']
    except Exception as ex:
      self._status = const.DB_STATUS['DISCONNECTED']
      raise ex

  def disconnect(self):
    if self.is_connected():
      self._helper.disconnect()
      self._status = const.DB_STATUS['DISCONNECTED']      

  def is_connected(self):
    return self._status == const.DB_STATUS['CONNECTED']
  
  def is_connecting(self):
    return self._status == const.DB_STATUS['CONNECTING']

  def get_dbtype(self):
    return self._dbType
  
  def execute_query(self):
    if not self.is_connected():
      raise RuntimeError('No connection.')
    self._querySql = self._helper.check_query(self._querySql)
    return self._helper.execute_query(self._querySql)

  def is_table_exist(self, table_name=None):
    if table_name is None:
      raise ValueError('table_name is necessary')
    if not self.is_connected():
      raise RuntimeError('No connection.')
    return self._helper.is_table_exist(table_name)

  def create_table(self, table_name=None, columns=[]):
    if table_name is None:
      raise ValueError('table_name is necessary')
    if not self.is_connected():
      raise RuntimeError('No connection.')
    if self._helper.is_table_exist(table_name):
      raise ValueError('table_name is exist')
    columns = list(map(self._check_columns, columns))
    self._helper.create_table(table_name, columns)

  def insert(self, table_name=None, columns=(), records=[]):
    if table_name is None:
      raise ValueError('table_name is necessary')
    if not self.is_connected():
      raise RuntimeError('No connection.')
    if not self._helper.is_table_exist(table_name) and self._dbType != const.DB_TYPE['INFLUXDB']:
      raise ValueError('table_name is not exist')
    self._helper.insert(table_name, columns, records)

  def _check_columns(self, col):
    if 'name' not in col:
      raise ValueError('name is necessary in {}'.format(col))
    if 'type' not in col:
      raise ValueError('type is necessary in {}'.format(col))
    return {
      'name': col['name'],
      'type': col['type'],
      'is_primary': True if 'is_primary' in col and col['is_primary'] else False,
      'is_not_null': True if 'is_not_null' in col and col['is_not_null'] else False,
    }