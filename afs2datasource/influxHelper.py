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
from influxdb import InfluxDBClient
import pandas as pd

class InfluxHelper():
  def __init__(self):
    self._connection = None

  def connect(self):
    if self._connection is None:
      data = utils.get_data_from_dataDir()
      username, password, host, port, database = utils.get_credential_from_dataDir(data)
      self._connection = InfluxDBClient(database=database, username=username, password=password, host=host, port=port)
  
  def disconnect(self):
    if self._connection:
      self._connection.close()

  async def execute_query(self, querySql):
    data = self._connection.query(querySql)
    data = list(data.get_points())
    data = pd.DataFrame(data)
    return data

  def check_query(self, querySql):
    if type(querySql) is not str:
      raise ValueError('querySql is invalid')
    return querySql

  def is_table_exist(self, table_name):
    return table_name in [measure['name'] for measure in self._connection.get_list_measurements()]

  def create_table(self, table_name, columns):
    return

  def insert(self, table_name, columns, records):
    records = list(map(lambda record: dict(zip(columns, record)), records))
    for record in records:
      self._connection.write_points([{
        'measurement': table_name,
        'fields': record
      }])
