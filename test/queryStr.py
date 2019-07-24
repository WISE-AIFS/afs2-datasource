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

import os
import json

config_file = [
  'config/postgres_internal.json',
  'config/postgres_external.json',
  'config/mongo_internal.json',
  'config/mongo_external.json',
  'config/influx_internal.json',
  'config/influx_external.json',
  'config/s3_internal.json',
  'config/s3_external.json',
  'config/apm_timerange.json',
  'config/azureblob.json'
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
  try:
    data = db.execute_query()
    print(data)
  except Exception as e:
    print(e)