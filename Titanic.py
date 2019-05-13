import os
import json
import pandas as pd
from afs2datasource import DBManager, constant

with open('config/influx_internal.json') as f:
  data = json.load(f)
  os.environ = data

# with pd.read_csv('../dataTrain.csv') as csvfile:
df = pd.read_csv('../dataTrain.csv')

db = DBManager()
db.connect()
# create table
columns = [
  {'name': 'index', 'type': 'INTEGER'},
  {'name': 'survived', 'type': 'FLOAT'},
  {'name': 'age', 'type': 'FLOAT'},
  {'name': 'embarked', 'type': 'INTEGER'},
  {'name': 'fare', 'type': 'FLOAT'},
  {'name': 'pclass', 'type': 'INTEGER'},
  {'name': 'sex', 'type': 'INTEGER'},
  {'name': 'title2', 'type': 'INTEGER'},
  {'name': 'ticket_info', 'type': 'INTEGER'},
  {'name': 'cabin', 'type': 'INTEGER'}
]

table_name = ''
if db.get_dbtype() == constant.DB_TYPE['POSTGRES']:
  table_name = 'afs.titanic'
elif db.get_dbtype() == constant.DB_TYPE['MONGODB']:
  table_name = 'titanic'
elif db.get_dbtype() == constant.DB_TYPE['INFLUXDB']:
  table_name = 'titanic'

# if not db.is_table_exist(table_name):  
db.create_table(table_name, columns)
# insert records
db.insert(table_name, list(df.columns.values), df.values)

# close db
db.disconnect()