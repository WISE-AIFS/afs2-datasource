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

import pandas as pd

from afs2datasource.helper import Helper
from afs2datasource.constant import DB_TYPE
from afs2datasource.utils import get_credential

from sqlalchemy import create_engine

class SQLServerHelper(Helper):
  def __init__(self, datadir):
    self._connection = None
    credential = datadir.get('credential')
    self._username, self._password, self._host, self._port, self._database = get_credential(credential)

  async def connect(self):
    if self._connection is None:
        uri = "mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server".format(
            username=self._username,
            password=self._password,
            host=self._host,
            port=self._port,
            database=self._database
        )
    connection = create_engine(uri, connect_args={'timeout': 5})
    connection.raw_connection()
    self._connection = connection
  
  def disconnect(self):
    if self._connection:
      self._connection.close()
      self._connection = None

  async def execute_query(self, querySql):
    result = self._connection.execute(querySql)
    result = [{col: value for col, value in row.items()} for row in result]
    df = pd.DataFrame(result)
    return df

  def check_query(self, querySql):
    if not querySql.strip().lower().startswith('select'):
      raise ValueError('Only support `SELECT` query.')
    return querySql


  def is_table_exist(self, table_name):
    rows = self._connection.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{}'".format(table_name))
    return len(rows) > 0


  def is_file_exist(self, **kwargs):
    raise NotImplementedError('OracleDB Datasource not implement.')


  def create_table(self, **kwargs):
    raise NotImplementedError('OracleDB Datasource not implement.')


  def insert(self, **kwargs):
    raise NotImplementedError('OracleDB Datasource not implement.')


  async def delete_table(self, **kwargs):
    raise NotImplementedError('OracleDB Datasource not implement.')


  def delete_record(self, **kwargs):
    raise NotImplementedError('OracleDB Datasource not implement.')
