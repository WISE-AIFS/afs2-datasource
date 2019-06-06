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
import os
import json

def get_data_from_dataDir():
  dataDir = os.getenv('PAI_DATA_DIR', {})
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
  endpoint = credentail.get('endpoint', None)
  access_key = credentail.get('accessKey', None)
  secret_key = credentail.get('secretKey', None)
  bucket = data.get('bucketName', None)
  credentail = data.get('credential', {})
  if not endpoint:
    raise AttributeError('No endpoint in credential')
  if not access_key:
    raise AttributeError('No accessKey in credential')
  if not secret_key:
    raise AttributeError('No secretKey in credential')
  if not bucket:
    raise AttributeError('No bucketName in credential')
  return endpoint, access_key, secret_key, bucket

