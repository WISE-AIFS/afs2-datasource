import os
import json
import afs2datasource.constant as const
import psycopg2
import pandas as pd

class PostgresHelper():
  def __init__(self):
    dataDir = os.getenv('PAI_DATA_DIR', {})
    if type(dataDir) is str:
      dataDir = json.loads(dataDir)
    self._connection = None

  def connect(self, username, password, host, port, database):
    self._connection = psycopg2.connect(database=database, user=username, password=password, host=host, port=port)
  
  def execute_query(self, querySql):
    cursor = self._connection.cursor()
    cursor.execute(querySql)
    columns = [desc[0] for desc in cursor.description]
    data = list(cursor.fetchall())
    data = pd.DataFrame(data=data, columns=columns)
    return data
  
  def check_query(self, querySql):
    if type(querySql) is not str:
      raise ValueError('querySql is invalid')
    # utils.check_sql(querySql)
    return querySql