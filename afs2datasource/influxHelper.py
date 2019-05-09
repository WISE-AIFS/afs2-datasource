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
    self._connection = InfluxDBClient(database=database, username=username, password=password, host=host, port=port)
  
  def execute_query(self, querySql):
    data = self._connection.query(querySql)
    data = list(data.get_points())
    data = pd.DataFrame(data)
    return data

  def check_query(self, querySql):
    if type(querySql) is not str:
      raise ValueError('querySql is invalid')
    return querySql