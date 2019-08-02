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
import base64
import asyncio
import pandas as pd
import afs2datasource.constant as const
import afs2datasource.utils as utils

class DBManager:
  def __init__(self, **config):
    if config:
      self._get_credential_from_config(config)
    dataDir = os.getenv('PAI_DATA_DIR', None)
    if dataDir:
      self._get_credential_from_env(dataDir)
    else:
      raise ValueError('No DB config.')
    self._status = const.DB_STATUS['DISCONNECTED']
    self._helper = self._create_helper(self._db_type)
    self.loop = asyncio.get_event_loop()

  def _get_credential_from_config(self, config):
    dataDir = {}
    db_type = config.get('db_type', None)
    if db_type == const.DB_TYPE['S3']:
      endpoint = config.get('endpoint', None)
      access_key = config.get('access_key', None)
      secret_key = config.get('secret_key', None)
      buckets = config.get('buckets', None)
      dataDir = {
        'type': db_type,
        'data': {
          'buckets': buckets,
          'credential': {
            'accessKey': access_key,
            'endpoint': endpoint,
            'secretKey': secret_key
          }
        }
      }
    elif db_type == const.DB_TYPE['APM']:
      username = config.get('username', None)
      password = config.get('password', None)
      password = base64.b64encode(password.encode('UTF-8')).decode('UTF-8')
      apmUrl = config.get('apmUrl', None)
      machineIdList = config.get('machineIdList', None)
      parameterList = config.get('parameterList', None)
      mongouri = config.get('mongouri', None)
      timeRange = config.get('timeRange', None)
      timeLast = config.get('timeLast',None)
      dataDir = {
        'type': db_type,
        'data': {
          'username': username,
          'password': password,
          'apmUrl': apmUrl,
          'machineIdList': machineIdList,
          'parameterList': parameterList,
          'timeRange': timeRange,
          'timeLast': timeLast,
          'credential': {
            'uri': mongouri
          }
        }
      }
    elif db_type == const.DB_TYPE['AZUREBLOB']:
      account_name = config.get('account_name', None)
      account_key = config.get('account_key', None)
      containers = config.get('containers', None)
      dataDir = {
        'type': db_type,
        'data': {
          'credential': {
            'accountName': account_name,
            'accountKey': account_key
          },
          'containers': containers
        }
      }
    else:
      username = config.get('username', None)
      password = config.get('password', None)
      host = config.get('host', None)
      port = config.get('port', None)
      database = config.get('database', None)
      querySql = config.get('querySql', None)
      collection = config.get('collection', None)
      dataDir = {
        'type': db_type,
        'data': {
          'collection': collection,
          'querySql': querySql,
          'credential': {
            'username': username,
            'password': password,
            'host': host,
            'port': port,
            'database': database
          }
        }
      }
    os.environ['PAI_DATA_DIR'] = json.dumps(dataDir)

  def _get_credential_from_env(self, dataDir):
    if type(dataDir) is str:
      dataDir = json.loads(dataDir)
    db_type = dataDir.get('type', None)
    if db_type is None:
      raise AttributeError('No type in dataDir')
    if db_type not in const.DB_TYPE.values():
      raise ValueError('{0} is not support'.format(db_type))

    self._status = const.DB_STATUS['DISCONNECTED']
    self._helper = self._create_helper(db_type)
    self._db_type = db_type

  def _create_helper(self, db_type):
    db_type = db_type.lower()
    if db_type == const.DB_TYPE['MONGODB']:
      import afs2datasource.mongoHelper as mongoHelper
      return mongoHelper.MongoHelper()
    elif db_type == const.DB_TYPE['POSTGRES']:
      import afs2datasource.postgresHelper as postgresHelper
      return postgresHelper.PostgresHelper()
    elif db_type == const.DB_TYPE['INFLUXDB']:
      import afs2datasource.influxHelper as influxHelper
      return influxHelper.InfluxHelper()
    elif db_type == const.DB_TYPE['S3']:
      import afs2datasource.s3Helper as s3Helper
      return s3Helper.s3Helper()
    elif db_type == const.DB_TYPE['APM']:
      import afs2datasource.apmDSHelper as apmDSHelper
      return apmDSHelper.APMDSHelper()
    elif db_type == const.DB_TYPE['AZUREBLOB']:
      import afs2datasource.azureBlobHelper as azureBlobHelper
      return azureBlobHelper.azureBlobHelper()
    else:
      raise ValueError('{} not support db_type'.format(db_type))

  def connect(self):
    try:
      if self.is_connected() or self.is_connecting():
        return
      self._status = const.DB_STATUS['CONNECTING']
      self._helper.connect()
      self._status = const.DB_STATUS['CONNECTED']
      return True
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
    return self._db_type

  def execute_query(self):
    data = utils.get_data_from_dataDir()
    if not self.is_connected():
      raise RuntimeError('No connection.')
    if self._db_type == const.DB_TYPE['S3']:
      query = data.get('buckets', [])
    elif self._db_type == const.DB_TYPE['AZUREBLOB']:
      query = data.get('containers', [])
    elif self._db_type == const.DB_TYPE['APM']:
      query = {
        'machine_list': data.get('machineIdList', []),
        'parameter_list': data.get('parameterList', []),
        'time_range': data.get('timeRange', []),
        'time_last': data.get('timeLast', [])
      }
    else:
      query = data.get('querySql', None)
      if not query and not(type(query) is dict):
        raise AttributeError('No querySql in dataDir[data]')
    query = self._helper.check_query(query)
    return self.loop.run_until_complete(self._helper.execute_query(query))

  def is_table_exist(self, table_name=None):
    if table_name is None:
      raise ValueError('table_name is necessary')
    if not self.is_connected():
      raise RuntimeError('No connection.')
    return self._helper.is_table_exist(table_name)

  def is_file_exist(self, table_name='', file_name=''):
    if self._db_type != const.DB_TYPE['S3'] and self._db_type != const.DB_TYPE['AZUREBLOB']:
      raise NotImplementedError('{} not implemented is_file_exist.'.format(self._db_type))
    if not table_name:
      raise ValueError('table_name is necessary')
    if not file_name:
      raise ValueError('file_name is necessary')
    return self._helper.is_file_exist(table_name, file_name)

  def create_table(self, table_name=None, columns=[]):
    if table_name is None:
      raise ValueError('table_name is necessary')
    if not self.is_connected():
      raise RuntimeError('No connection.')
    if self._helper.is_table_exist(table_name):
      raise ValueError('table_name is exist')
    columns = list(map(self._check_columns, columns))
    try:
      self._helper.create_table(table_name=table_name, columns=columns)
      return True
    except Exception as e:
      raise Exception(e)

  def insert(self, table_name=None, columns=(), records=[], source='', destination=''):
    try:
      if not table_name:
        raise ValueError('table_name is necessary')
      if not self.is_connected():
        raise RuntimeError('No connection.')
      if not self._helper.is_table_exist(table_name) and self._db_type != const.DB_TYPE['INFLUXDB']:
        raise ValueError('table_name is not exist')
      if self._db_type == const.DB_TYPE['S3'] or self._db_type == const.DB_TYPE['AZUREBLOB']:
        if not source or not destination:
          raise ValueError('source and destination is necessary')
        if destination.endswith('/'):
          raise ValueError('destination cannot end with "/"')
        self._helper.insert(table_name=table_name, source=source, destination=destination)
        return True
      else:
        records = [[None if pd.isnull(value) else value for value in record] for record in records]
        return self._helper.insert(table_name=table_name, columns=columns, records=records)
    except Exception as e:
      raise Exception(e)

  # def delete_table(self, table_name=''):

  def delete_file(self, table_name='', file_name=''):
    if self._db_type != const.DB_TYPE['S3']:
      raise NotImplementedError('{} not implemented.'.format(self._db_type))
    if not table_name:
      raise ValueError('table_name is necessary')
    if not file_name:
      raise ValueError('file_name is necessary')
    if not self.is_file_exist(table_name, file_name):
      raise FileNotFoundError('{} file is not found'.format(file_name))
    return self._helper.delete_file(table_name, file_name)

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