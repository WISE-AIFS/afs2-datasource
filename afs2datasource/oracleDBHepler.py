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

import cx_Oracle
import pandas as pd

from afs2datasource.helper import Helper
from afs2datasource.constant import DB_TYPE
from afs2datasource.utils import get_credential

class OracleDBHelper(Helper):
  def __init__(self, datadir):
    self._connection = None
    credential = datadir.get('credential')
    self._username, self._password, self._host, self._port, self._dsn = get_credential(credential)

    # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    self._tns = cx_Oracle.makedsn(
      r'{}'.format(self._host), 
      self._port, 
      service_name=r'{}'.format(self._dsn))

  async def connect(self):
    if self._connection is None:
      self._connection = cx_Oracle.connect(
        user=r'{}'.format(self._username),
        password=r'{}'.format(self._password),
        dsn=r'{}'.format(self._tns)
      )

      cursor = self._connection.cursor()
      cursor.execute("SELECT table_name FROM user_tables")

  
  def disconnect(self):
    if self._connection:
      self._connection.close()
      self._connection = None


  async def execute_query(self, querySql):
    cursor = self._connection.cursor()
    cursor.execute(querySql)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    data = pd.DataFrame(rows, columns=columns)
    return data


  def check_query(self, querySql):
    if not querySql.strip().lower().startswith('select'):
      raise ValueError('Only support `SELECT` query.')
    return querySql


  def is_table_exist(self, table_name):
    cursor = self._connection.cursor()
    cursor.execute("SELECT table_name FROM user_tables WHERE table_name='{}'".format(table_name))
    rows = cursor.fetchall()
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
