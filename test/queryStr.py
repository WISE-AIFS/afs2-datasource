import os
import json

# read json file
with open('config/influx_external.json') as f:
  data = json.load(f)
  os.environ = data

# import AFS2DataSource
from afs2datasource import DBManager
db = DBManager()
db.connect()

# query sql
data = db.execute_query()
print(data)