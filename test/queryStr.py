import os
import json

config_file = [
  'config/postgres_internal.json',
  'config/postgres_external.json',
  'config/mongo_internal.json',
  'config/mongo_external.json',
  'config/influx_internal.json',
  'config/influx_external.json',
]

# read json file
for config in config_file: 
  print('-----{}-----'.format(config))
  with open(config) as f:
    data = json.load(f)
    os.environ = data

  # import AFS2DataSource
  from afs2datasource import DBManager
  db = DBManager()
  db.connect()

  # query sql
  data = db.execute_query()
  print(data)