import afs2datasource.constant as const
import afs2datasource.utils as utils
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

class PostgresHelper():
  def __init__(self):
    self._connection = None
    self._username = ''

  def connect(self):
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
    return querySql

  def is_table_exist(self, table_name):
    cursor = self._connection.cursor()
    table_name = table_name.split('.')
    if len(table_name) < 2:
      raise ValueError('table_name is invalid. ex.{schema}.{table}')
    schema = table_name[0]
    table = table_name[1]
    command = "select * from information_schema.tables"
    cursor.execute(command)
    for d in cursor.fetchall():
      if d[1] == schema and d[2] == table:
        return True
    return False

  def create_table(self, table_name, columns):
    table_name = table_name.split('.')
    if len(table_name) < 2:
      raise ValueError('table_name is invalid. ex.{schema}.{table}')
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
