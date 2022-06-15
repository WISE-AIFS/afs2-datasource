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

import asyncio
import pandas as pd
import motor.motor_asyncio
from pymongo import ASCENDING
from influxdb import InfluxDBClient
from dateutil.parser import parse
import afs2datasource.utils as utils
import afs2datasource.constant as const
from afs2datasource.helper import Helper
from datetime import datetime, timedelta

class DataHubHelper(Helper):
  def __init__(self, dataDir):
    self._connection = None
    data = utils.get_data_from_dataDir(dataDir)
    self._mongo_url, self._influx_url = utils.get_datahub_credential_from_dataDir(data)
    self._db = ''
    self._db_type = const.DB_TYPE['MONGODB'] if self._mongo_url else const.DB_TYPE['INFLUXDB']

  async def connect(self):
    if self._connection is None:
      if self._db_type == const.DB_TYPE['MONGODB']:
        self._connection = motor.motor_asyncio.AsyncIOMotorClient(self._mongo_url)
        await self._connection.server_info()
        self._db = self._mongo_url.split('/')[-1]
      elif self._db_type == const.DB_TYPE['INFLUXDB']:
        username, password, host, port, database = utils.get_credential_from_uri(self._influx_url)
        _conn = InfluxDBClient(database=database, username=username, password=password, host=host, port=port, timeout=5)
        _conn.get_list_database()
        self._connection = InfluxDBClient(database=database, username=username, password=password, host=host, port=port)

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
    
    # check datahub config
    # TODO: 
    return query

  async def execute_query(self, query):
    # execute query by each datahub config
    df_list = await asyncio.gather(*[self._execute_query(query['time_range'], q) for q in query['config']]) 
    resp_dict = {}
    for idx, value in enumerate(df_list):
      resp_dict[query['config'][idx]['name']] = value
    
    if len(resp_dict) == 1:
      _, resp_dict = resp_dict.popitem()
    return resp_dict


  async def _execute_query(self, time_range, query):
    if not len(query['tags']):
      return pd.DataFrame(columns=['ts'])

    # according to db type
    if self._db_type == const.DB_TYPE['MONGODB']:
      # generate sql
      ts = list(map(lambda ts: {'ts': {'$gte': ts['start'], '$lte': ts['end']}}, time_range))
      sql = {
        'deviceId': query['device_id'],
        '$or': ts
      }
      # query data
      collection = 'datahub_HistRawData_{node_id}'.format(
        node_id=query['node_id']
      )
      projection = { tag: 1 for tag in query['tags'] }
      projection.update({ '_id': 0, 'ts': 1 })
      docs = self._connection[self._db][collection].find(sql, projection).sort('ts',ASCENDING)
      data = await docs.to_list(length=None)
      data = pd.DataFrame(data, columns=['ts'] + query['tags'])
    else: # influx db
      measurement = 'HistRawData_{node_id}_{device_id}'.format(
        node_id=query['node_id'],
        device_id=query['device_id'],
      )

      tags = query['tags']

      time_conditions = []
      for ts in time_range:
        condition = ''
        if 'start' in ts:
          # nano seconds
          condition += 'time >= {}000'.format(int(ts['start'].timestamp() * 1000 * 1000))
        if 'end' in ts:
          if condition:
            condition += ' and '
          condition += 'time <= {}000'.format(int(ts['end'].timestamp() * 1000 * 1000))
        time_conditions.append('({})'.format(condition))

      query = 'SELECT * FROM "{measurement}" WHERE ({time_conditions}) AND ({tag_conditions})'.format(
        measurement=measurement,
        time_conditions=' OR '.join(time_conditions),
        tag_conditions=' OR '.join(list(map(lambda tag: "id='{}'".format(tag), tags)))
      )

      records = list(self._connection.query(query).get_points())

      data = {}
      for record in records:
        if record['time'] not in data:
          data[record['time']] = {'ts': record['time']}
        data[record['time']][record['id']] = record['tval'] if ('tval' in record and record['tval'] is not None) else record['val']
      data = pd.DataFrame(data.values(), columns=['ts'] + tags)
    
    return data

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
