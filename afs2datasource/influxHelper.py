import os
import json
import afs2datasource.constant as const
import afs2datasource.utils as utils
from influxdb import InfluxDBClient
import pandas as pd

class InfluxHelper():
  def __init__(self):
    dataDir = os.getenv('PAI_DATA_DIR', {})
    if type(dataDir) is str:
      dataDir = json.loads(dataDir)
    self._connection = None

  def connect(self, username, password, host, port, database):
    if self._connection is None:
      self._connection = InfluxDBClient(database=database, username=username, password=password, host=host, port=port)
  
  def disconnect(self):
    if self._connection:
      self._connection.close()

  def execute_query(self, querySql):
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
