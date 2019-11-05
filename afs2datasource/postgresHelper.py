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

import afs2datasource.constant as const
import afs2datasource.utils as utils
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from functools import wraps

class PostgresHelper():
  def __init__(self):
    self._connection = None
    self._username = ''

  async def connect(self):
    if self._connection is None:
      data = utils.get_data_from_dataDir()
      username, password, host, port, database = utils.get_credential_from_dataDir(data)
      self._connection = psycopg2.connect(database=database, user=username, password=password, host=host, port=port)
      self._username = username
  
  def disconnect(self):
    if self._connection:
      self._connection.close()
      self._connection = None
      self._username = ''
  
  async def execute_query(self, querySql):
    cursor = self._connection.cursor()
    cursor.execute(querySql)
    columns = [desc[0] for desc in cursor.description]
    data = list(cursor.fetchall())
    data = pd.DataFrame(data=data, columns=columns)
    return data
  
  def check_query(self, querySql):
    if type(querySql) is not str:
      raise ValueError('querySql is invalid')
    return querySql

  def check_table_name(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
      table_name = kwargs.get('table_name')
      if not table_name:
        raise ValueError('table_name is necessary')
      check_table_name = table_name.split('.')
      if len(check_table_name) < 2:
        raise ValueError('table_name is invalid. ex.{schema}.{table}')
      return func(self, *args, **kwargs)
    return wrapper

  @check_table_name
  def is_table_exist(self, table_name):
    cursor = self._connection.cursor()
    table_name = table_name.split('.')
    schema = table_name[0]
    table = table_name[1]
    command = "select * from information_schema.tables"
    cursor.execute(command)
    for d in cursor.fetchall():
      if d[1] == schema and d[2] == table:
        return True
    return False

  def is_file_exist(self, table_name, file_name):
    raise NotImplementedError('Postgres not implement.')

  @check_table_name
  def create_table(self, table_name, columns):
    table_name = table_name.split('.')
    schema = table_name[0]
    table = table_name[1]
    cursor = self._connection.cursor()
    command = 'CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION "{username}"'.format(schema=schema, username=self._username)
    cursor.execute(command)
    command = 'CREATE TABLE {schema}.{table} ('.format(schema=schema, table=table)
    fields = []
    for col in columns:
      field = '{name} {type}'.format(name=col['name'], type=col['type'])
      if col['is_primary']:
        field += ' PRIMARY KEY'
      if col['is_not_null']:
        field += ' NOT NULL'
      fields.append(field)
    command += ','.join(fields) + ')'

    cursor.execute(command)
    self._connection.commit()

  @check_table_name
  def insert(self, table_name, columns, records):
    cursor = self._connection.cursor()
    for record in records:
      if len(record) != len(columns):
        raise IndexError('record {} and columns do not match'.format(record))
    records = [tuple(record) for record in records]
    command = 'INSERT INTO {table_name}('.format(table_name=table_name)
    command += ','.join(columns) + ') VALUES %s'
    execute_values(cursor, command,(records))
    self._connection.commit()

  @check_table_name
  async def delete_table(self, table_name):
    cursor = self._connection.cursor()
    command = 'DROP TABLE IF EXISTS {table_name}'.format(table_name=table_name)
    cursor.execute(command)
    self._connection.commit()

  @check_table_name
  def delete_record(self, table_name, condition):
    cursor = self._connection.cursor()
    command = 'DELETE FROM {table_name} WHERE {condition}'.format(table_name=table_name, condition=condition)
    cursor.execute(command)
    self._connection.commit()
