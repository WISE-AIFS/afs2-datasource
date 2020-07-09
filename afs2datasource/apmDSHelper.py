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
import asyncio
import requests
import pandas as pd
import motor.motor_asyncio
from pymongo import ASCENDING
from urllib.parse import urljoin
from dateutil.parser import parse
import afs2datasource.utils as utils
import afs2datasource.constant as const
from datetime import datetime, timedelta

RETRY = 5

class APMDSHelper():
  def __init__(self):
    self._connection = None
    data = utils.get_data_from_dataDir()
    self._username, self._password, self._apm_url, self._mongo_url, self._influx_url = utils.get_apm_credential_from_dataDir(data)
    self._db = ''
    if self._influx_url:
      self._db_type = const.DB_TYPE['INFLUXDB']
    elif self._mongo_url:
      self._db_type = const.DB_TYPE['MONGODB']
    else:
      self._db_type = const.DB_TYPE['APM']

  async def connect(self):
    if self._connection is None:
      if self._db_type == const.DB_TYPE['MONGODB']:
        self._connection = motor.motor_asyncio.AsyncIOMotorClient(self._mongo_url)
        data = await self._connection.server_info()
        self._db = self._mongo_url.split('/')[-1]
      self._token = self._get_token()

  def disconnect(self):
    if self._connection:
      self._connection.close()
      self._connection = None
      self._db = ''

  def check_query(self, query):
    # check time
    if not query['time_range'] and not query['time_last']:
      raise ValueError('time_range and time_list is empty')

    if query['time_range']:
      for time in query['time_range']:
        if time.get('start') is None or time.get('end') is None:
          raise ValueError('time_range is invalid')
        time['start'] = parse(time.get('start'))
        time['end'] = parse(time.get('end')) + timedelta(days=1)
    elif query['time_last']:
      days = int(query['time_last'].get('lastDays', 0))
      hours = int(query['time_last'].get('lastHours', 0))
      mins = int(query['time_last'].get('lastMins', 0))
      if days + hours + mins == 0:
        raise ValueError('time_last is invalid')
      now = datetime.now()
      query['time_range'] = [{
        'start': now - timedelta(days=days, hours=hours, minutes=mins),
        'end': now
      }]
      del query['time_last']
    
    # check apm config
    # check machines
    apm_configs = query.get('apm_config', [])
    for apm_config in apm_configs:
      name = apm_config.get('name', None)
      if not name:
        raise ValueError('name in apm_config is empty')
      machines = apm_config.get('machines', [])
      for machine in machines:
        if not 'id' in machine:
          raise ValueError('machine id is empty')
    return query

  def _get_token(self):
    url = urljoin(self._apm_url, 'api-apm/auth/login')
    body = {
      'userName': self._username,
      'password': self._password
    }
    retry = 0
    while retry < RETRY:
      try:
        resp = requests.post(
          url,
          json=body,
          timeout=3
        )
        if resp.status_code == 200:
          resp = resp.json()
          return resp['accessToken']
        else:
          message = json.loads(resp.content.decode('UTF-8'))
          raise Exception('Try SSO Login Failed {}.'.format(message['message']))
      except requests.exceptions.Timeout:
        retry = retry + 1
    raise Exception('Try SSO Login Failed.')

  async def execute_query(self, query):
    # execute query by each apm config
    resp = await asyncio.gather(*[self._execute_query_by_config(query['time_range'], q) for q in query['apm_config']]) 
    resp_dict = {}
    for idx, value in enumerate(resp):
      resp_dict[query['apm_config'][idx]['name']] = value
    
    if len(resp_dict) == 1:
      _, resp_dict = resp_dict.popitem()
    return resp_dict

  async def _execute_query_by_config(self, time_range, query):
    # if self._db_type == const.DB_TYPE['APM']:
    #   query_list = []
    #   for machine in query.get('machines', []):
    #     for parameter in query.get('parameters', []):
    #       for time in time_range:
    #         query_list.append({
    #           'node_id': machine['id'],
    #           'sensor_type': 'monitor',
    #           'sensor_name': parameter,
    #           'start_ts': time['start'],
    #           'end_ts': time['end']
    #         })
    #   df_list = await asyncio.gather(*[self._execute_query_by_apm(query) for query in query_list])

    # else:
    # format the apm config
    machine_list = query.get('machines', [])
    query_list = []
    for machine in machine_list:
      query_list.append({
        'id': machine['id'],
        'parameters': query.get('parameters', [])
      })
    # execute query by each machine in apm config
    df_list = await asyncio.gather(*[self._execute_query_by_machine(time_range, query) for query in query_list])
    df_list = [inner for outer in df_list for inner in outer]
    # join dataframe
    result = self._combine_data(df_list)
    return result

  async def _execute_query_by_machine(self, time_range, query):
    # query scada config
    query_list = self._get_machine_info(query)
    # execute query by each scada config, and return dataframe
    df_list = await asyncio.gather(*[self._execute_query(time_range, query) for query in query_list]) 
    return df_list

  def _get_machine_info(self, query):
    # get apm node info by node id
    url = urljoin(self._apm_url, 'api-apm/topo/node/detail/info')
    header = {'Authorization': 'Bearer ' + self._token}
    params = {'id': query['id']}
    query_list = []

    # no tag_name is query
    if len(query['parameters']) == 0:
      return query_list

    retry = 0
    while retry < RETRY:
      resp = requests.get(
        url,
        headers=header,
        params=params
      )
      if resp.status_code == 200:
        resp = resp.json()
        name = resp.get('name', '')
        dtInstance = resp.get('dtInstance', {})
        property = dtInstance.get('property', {})
        iotSense = property.get('iotSense', {})
        if not ('deviceId' in iotSense and 'groupId' in iotSense):
          raise ValueError('APM response format error')
        scada_id = iotSense['groupId']
        device_id = iotSense['deviceId'].split('@')[-1]
        feature = dtInstance.get('feature', {})
        monitor = feature.get('monitor', [])
        for parameter in query['parameters']:
          tag = next((t for t in monitor if t['name'] == parameter), {})
          if tag:
            query_list.append({
              'scada_id': scada_id,
              'device_id': device_id,
              'tag_name': tag['tag'].split('@')[-1],
              'parameter': parameter
            })
          else:
            raise ValueError('Machine {0} do not have {1} parameter.'.format(name, parameters))
        return query_list
      else:
        retry = retry + 1
    raise RuntimeError('Get Machine Id {0} detail failed: {1}'.format(query['id'], resp))

  async def _execute_query(self, time_range, query):
    # according to db type
    if self._db_type == const.DB_TYPE['MONGODB']:
      # generate sql
      ts = list(map(lambda ts: {'ts': {'$gte': ts['start'], '$lte': ts['end']}}, time_range))
      sql = {
        't': query['tag_name'],
        '$or': ts
      }
      # query data
      collection = 'datahub_HistRawData_{node_id}_{device_id}'.format(
        node_id=query['scada_id'],
        device_id=query['device_id']
      )
      docs = self._connection[self._db][collection].find(sql, {'_id':0, 't':0}).sort('ts',ASCENDING)
      data = await docs.to_list(length=None)
      data = pd.DataFrame(data, columns=['ts', 'v']).rename(columns={'v': query['parameter']})
    else: # influx db
      data = pd.DataFrame(columns=['ts', 'v']).rename(columns={'v': query['parameter']})
    return data
  
  async def _execute_query_by_apm(self, query):
    node_id = query['node_id']
    sensor_type = query['sensor_type']
    sensor_name = query['sensor_name']
    start_ts = query['start_ts']
    end_ts = query['end_ts']

    paginator = True
    while paginator:
      url = urljoin(self._apm_url, 'api-apm/hist/raw/data')
      params = {
        'nodeId': node_id,
        'sensorType': sensor_type,
        'sensorName': sensor_name,
        'startTs': start_ts,
        'endTs': end_ts,
        'count': 10000
      }
      header = {'Authorization': 'Bearer ' + self._token}
      resp = requests.get(
        url,
        headers=header,
        params=params
      )
      print(resp)

  def _combine_data(self, df_list):
    for idx, df in enumerate(df_list):
      if idx == 0:
        result = df
      else:
        result = pd.merge(result, df, how='outer', on='ts').sort_values(by=['ts'])
    return result

  def is_table_exist(self, table_name):
    raise NotImplementedError('APMDataSource not implement.')

  def is_file_exist(self, table_name, file_name):
    raise NotImplementedError('APMDataSource not implement.')

  def create_tabe(self, table_name):
    raise NotImplementedError('APMDataSource not implement.')

  def insert(self,table_name, columns, records):
    raise NotImplementedError('APMDataSource not implement.')

  def delete_table(self, table_name):
    raise NotImplementedError('APMDataSource not implement.')

  def create_table(self, table_name, columns):
    raise NotImplementedError('APMDataSource not implement.')

  def delete_record(self, table_name, condition):
    raise NotImplementedError('APMDataSource not implement.')
