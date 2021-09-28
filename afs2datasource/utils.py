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

import re
import json

def get_data_from_dataDir(dataDir):
  if type(dataDir) is str:
    dataDir = json.loads(dataDir)
  data = dataDir.get('data', {})
  return data

def get_credential_from_uri(uri):
  uri_pattern = r'^(.*):\/\/(.*):(.*)@(.*)\/(.*)$'
  result = re.findall(uri_pattern, uri)[0]
  protocal = result[0]
  username = result[1]
  password = result[2]
  temp = result[3].split(',')[0]
  host = temp.split(':')[0]
  port = temp.split(':')[1]
  database = result[4]
  return username, password, host, port, database

def get_credential_from_dataDir(data):
  credential = data.get('credential', None)
  if credential:
    return get_credential(credential)
  else:
    uri = data.get('externalUrl', None)
    if not uri:
      raise AttributeError('No externalUrl in dataDir[data]')
    return get_credential_from_uri(uri)

def get_credential(credential):
  username = credential.get('username', None)
  password = credential.get('password', None)
  host = credential.get('host', None)
  port = credential.get('port', None)
  database = credential.get('database', None)
  if username is None:
    raise AttributeError('No username in credential')
  if password is None:
    raise AttributeError('No password in credential')
  if host is None:
    raise AttributeError('No host in credential')
  if port is None:
    raise AttributeError('No port in credential')
  if database is None:
    raise AttributeError('No database in credential')
  # print('username: {0}\npassword: {1}\nhost: {2}\nport: {3}\ndatabase: {4}'.format(username, password, host, port, database))
  return username, password, host, port, database

def get_s3_credential(data):
  credential = data.get('credential', {})
  endpoint = credential.get('endpoint', None)
  access_key = credential.get('accessKey', None)
  secret_key = credential.get('secretKey', None)
  is_verify = credential.get('is_verify', False)
  if not access_key:
    raise AttributeError('No accessKey in credential')
  if not secret_key:
    raise AttributeError('No secretKey in credential')
  return endpoint, access_key, secret_key, is_verify

def get_azure_blob_credential(data):
  credential = data.get('credential', {})
  account_name = credential.get('accountName', None)
  account_key = credential.get('accountKey', None)
  if not account_name:
    raise AttributeError('No accountName in credential')
  if not account_key:
    raise AttributeError('No accountKey in credential')
  return account_name, account_key

def get_apm_credential_from_dataDir(data):
  username = data.get('username', None)
  password = data.get('password', None)
  apm_url = data.get('apmUrl', None)
  credential = data.get('credential', {})
  mongo_url = credential.get('uri', None)
  influx_url = credential.get('influx_uri', None)

  if not username:
    raise AttributeError('No username in data')
  if not password:
    raise AttributeError('No password in data')
  if not apm_url:
    raise AttributeError('No apmUrl in data')
  if not mongo_url:
    raise AttributeError('No mongouri in credential')

  return username, password, apm_url, mongo_url, influx_url

def get_datahub_credential_from_dataDir(data):
  credential = data.get('credential', {})
  uri = credential.get('uri', '')
  
  mongo_url, influx_url = None, None

  if uri.startswith('mongodb://'):
    mongo_url = uri
  elif uri.startswith('influxdb://'):
    influx_url = uri

  if not mongo_url and not influx_url:
    raise AttributeError('No uri in credential')

  return mongo_url, influx_url

def is_table_name_invalid(table_name):
  """
  Bucket names must be at least 3 and no more than 63 characters long.
  Bucket names must not contain uppercase characters or underscores.
  Bucket names must start with a lowercase letter or number.
  """

  reg = "^[a-z0-9-]{3,63}$"
  return re.match(reg, table_name)