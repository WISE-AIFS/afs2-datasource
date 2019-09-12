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

import json
import os
import requests as req
from urllib.parse import urljoin
import motor.motor_asyncio
import asyncio
from pymongo import ASCENDING
import pandas as pd
import datetime

class APMDSHelper():
  def __init__(self):
    self._connection = None
    self._db = ''
    dataDir = os.getenv('PAI_DATA_DIR', None)
    if dataDir is not None:
      if type(dataDir) is str:
        dataDir = json.loads(dataDir)
      self.__login_data = {"userName": dataDir['data']['username'], "password": dataDir['data']['password']}
      self.apm_url = dataDir['data']['apmUrl']
      # self.machine_list = dataDir['data']['machineIdList']
      self.machine_list = self.apm_config_filter(dataDir['data']['apm_config'], 'machine_id')
      # self.parameter_list = dataDir['data']['apm_config']['parameters']
      self.parameter_list = self.apm_config_filter(dataDir['data']['apm_config'], 'parameter')
      self.__mongo_credentials = dataDir['data']['credential']['uri']
      if dataDir['data']['timeRange'] != []:
        self.time_range = dataDir['data']['timeRange']
      elif dataDir['data']['timeLast'] != []:
        new_start_time = (datetime.datetime.now(
        ) - datetime.timedelta(days=int(dataDir['data']['timeLast']['lastDays']), hours=int(dataDir['data']['timeLast']['lastHours']), minutes=int(dataDir['data']['timeLast']['lastMins']))).strftime('%Y-%m-%d')
        self.time_range = [{'start': new_start_time,'end': datetime.datetime.now().strftime('%Y-%m-%d')}]
    else:
      raise AssertionError(
        "Environment parameters need apm_username={username}, apm_password={password}, apm_url={apmUrl}, machine_id_list={machineIdList}, parameter_list={parameterList}, mongo_uri={mongouri} and time_range={timeRange}".format(
            username=self.__login_data['userName'], password=self.__login_data['password'], apmUrl=self.apm_url, machineIdList=self.machine_list, parameterList=self.parameter_list, mongouri=self.__mongo_credentials, timeRange=self.time_range)
      )

  def apm_config_filter(self, apm_config, select_type):
    if select_type is 'machine_id':
      machine_id_list = []
      for i, e in enumerate(apm_config):
        for m_i, m_e in enumerate(e['machines']):
          machine_id_list.append(m_e['id'])
      return machine_id_list
    elif select_type is 'parameter':
      parameter_list = []
      for i, e in enumerate(apm_config):
        for p_i, p_e in enumerate(e['parameters']):
          parameter_list.append(p_e)
      return parameter_list

  def connect(self):
    if self._connection is None:
      self._connection = motor.motor_asyncio.AsyncIOMotorClient(self.__mongo_credentials)
      self._db = self.__mongo_credentials.split('/')[-1]

  def disconnect(self):
    if self._connection:
      self._connection.close()
      self._connection = None
      self._db = ''

  def check_query(self, query):
    if not query['machine_list']:
      raise ValueError('machine_list is invalid')
    if not query['parameter_list']:
      raise ValueError('parameter_list is invalid')
    if not query['time_range'] and not query['time_last']:
      raise ValueError('time_range is invalid')
    if query['time_range']:
      for time in query['time_range']:
        if time.get('start') is None or time.get('end') is None:
          raise ValueError('time_range is invalid')
    elif query['time_last']:
      if query['time_last'].get('lastDays') is None or query['time_last'].get('lastHours') is None or query['time_last'].get('lastMins') is None:
        raise ValueError('time_last is invalid')
    return query

  def get_token(self):
    login_url = urljoin(self.apm_url, '/auth/login')
    accept_header = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    counts = 0
    while counts < 5:
      try:
        login_information = req.post(login_url, data=json.dumps(self.__login_data), headers=accept_header, timeout=3)
        if login_information.status_code is 200:
          return json.loads(login_information.content.decode('UTF-8'))
        else:
          message = json.loads(login_information.content.decode('UTF-8'))
          raise Exception('Try SSO Login Failed {}.'.format(message['message']))
      except Exception as e:
        raise Exception('login failed. error: {}'.format(e))
    raise Exception('Try SSO Login Failed {} times.'.format(counts))

  def get_machine_detail(self):
    self.machine_content = []
    apm_token = self.get_token()
    get_node_detail_url = urljoin(self.apm_url, '/topo/node/detail/info')
    header = {'Accept': 'application/json', 'Authorization': 'Bearer ' + apm_token['accessToken']}
    for mid in self.machine_list:
      try:
        machineDetail = req.get(get_node_detail_url, headers=header, params='id='+str(mid))
        if (machineDetail.status_code == 200):
          dtInstance = json.loads(machineDetail.text)['dtInstance']
          self.reorganize_detail(dtInstance)
        else:
          raise RuntimeError('Get Machine {0} detail failed: {1}'.format(min, machineDetail.status_code))
      except Exception as e:
        raise RuntimeError('Get Machine {0} detail failed: {1}'.format(min, e))

  def reorganize_detail(self,dtInstance):
    tags = []
    compact = []
    dtFeature = dtInstance['feature']['monitor']
    dtProperty = dtInstance['property']['iotSense']
    device = dtProperty['deviceId'].split('@')[-1]
    dtFeature = list(filter(lambda f: f['name'] in self.parameter_list, dtFeature))
    tags = []
    for t in dtFeature:
      tags.append({
        's': dtProperty['groupId'],
        't': device + '\\' + t['tag'].split('@')[-1],
      })
    self.machine_content += tags

  async def generate_querySql(self):
    timeSql = []
    querySql = []
    for tr in self.time_range:
      startTS = datetime.datetime.strptime(tr['start'], '%Y-%m-%d')
      endTS = datetime.datetime.strptime(tr['end'], '%Y-%m-%d') + datetime.timedelta(days=1)
      timeSql.append({'ts': {'$gte': startTS, '$lte': endTS}})
    for mc in self.machine_content:
      mc['$or'] = timeSql
      querySql.append(mc)
    self.data_container = []
    await asyncio.wait([self.execute('scada_HistRawData', sql) for sql in querySql])
    return self.combine_data(self.data_container)
    # return self.execute('scada_HistRawData', querySql)

  async def execute_query(self, query):
    self.get_machine_detail()
    return await self.generate_querySql()

  async def execute(self, collection, query_sql):
    documents = self._connection[self._db][collection].find(query_sql, {'_id':0, 's':0, 't':0}).sort('ts',ASCENDING)
    data = await documents.to_list(length=None)
    count_data = len(data)
    data = pd.DataFrame(data=data)
    if count_data is not 0:
      data.columns = ['ts', query_sql['t']]
      self.data_container.append(data)

  def combine_data(self, container):
    if len(container) == 0:
      return pd.DataFrame()
    self.results = container[0]
    for contain in range(len(container)):
      if contain == 0:
        continue
      self.results = pd.merge(self.results, container[contain], how='outer', on='ts').sort_values(by=['ts'])
    return self.results

  def is_table_exist(self, table_name):
    raise NotImplementedError('APMDataSource not implement.')

  def is_file_exist(self, table_name, file_name):
    raise NotImplementedError('APMDataSource not implement.')

  def create_tabe(self, table_name):
    raise NotImplementedError('APMDataSource not implement.')

  def insert(self,table_name, columns, records):
    raise NotImplementedError('APMDataSource not implement.')
