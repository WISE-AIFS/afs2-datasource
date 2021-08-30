import os
from afs2datasource import DBManager, constant

# --------- config --------- #
db_type = constant.DB_TYPE['MYSQL']
username = ''
password = ''
host = ''
port = 3306
database = ''
table_name = 'titanic'

# -------------------------- #

manager = DBManager(db_type=db_type,
    username=username,
    password=password,
    host=host,
    port=port,
    database=database,
    querySql=''
)

try:
  # connect to MySQL
  is_success = manager.connect()
  print('Connection: {}'.format(is_success))

  # check if table is exist
  is_table_exist = manager.is_table_exist(table_name)

  # create table
  if not is_table_exist:
    columns = [
      {'name': 'passenger_id', 'type': 'INTEGER'},
      {'name': 'survived', 'type': 'INTEGER'},
      {'name': 'pclass', 'type': 'INTEGER'},
      {'name': 'name', 'type': 'VARCHAR(255)'},
      {'name': 'sex', 'type': 'VARCHAR(255)'},
      {'name': 'age', 'type': 'INTEGER'},
      {'name': 'sib_sp', 'type': 'INTEGER'},
      {'name': 'parch', 'type': 'VARCHAR(255)'},
      {'name': 'ticket', 'type': 'VARCHAR(255)'},
      {'name': 'fare', 'type': 'FLOAT'},
      {'name': 'cabin', 'type': 'VARCHAR(255)'},
      {'name': 'embarked', 'type': 'VARCHAR(255)'},
    ]
    print('Create Table {0} successfully: {1}'.format(table_name, manager.create_table(table_name, columns=columns)))
    is_table_exist = manager.is_table_exist(table_name)
  print('Table {0} exist: {1}'.format(table_name, is_table_exist))

  # insert record
  import pandas
  filename = 'test/titanic.csv'
  df = pandas.read_csv(filename)
  columns = list(df.columns)
  records = df.values
  manager.insert(table_name=table_name, columns=columns, records=records)
  print('Insert Data: {}'.format(filename))

  # Query record
  querySql = 'SELECT * FROM {}'.format(table_name)
  print('Execute Query: {}'.format(querySql))
  data = manager.execute_query(querySql)
  print(data)

  # Delete row
  condition = 'passenger_id = 1'
  print('Delete Row with condition: {}'.format(condition))
  is_success = manager.delete_record(table_name=table_name, condition=condition)
  print('Delete Row: {}'.format(is_success))
  
  # Check if successed
  if is_success:
    querySql = 'SELECT * FROM {table_name} WHERE {condition}'.format(
      table_name=table_name,
      condition=condition
    )
    data = manager.execute_query(querySql)
    if len(data):
      raise ValueError('Delete Row Failed')
    else:
      print('Delete Row Successfully')

  # Delete Table
  is_success = manager.delete_table(table_name=table_name)
  print('Delete Table {}: {}'.format(table_name, is_success))
  if is_success:
    is_exist = manager.is_table_exist(table_name=table_name)
    print('Check if Table {} is exist: {}'.format(table_name, is_exist))

  manager.disconnect()

except Exception as e:
  print(e)
