import re

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

def get_credential(data):
  dbType = data.get('dbType', None)
  credential = data.get('credential', None)
  if credential:
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
    # print(username, password, host, port, database)
    return username, password, host, port, database
  else:
    uri = data.get('externalUrl', None)
    if not uri:
      raise AttributeError('No externalUrl in dataDir[data]')
    return get_credential_from_uri(uri)
