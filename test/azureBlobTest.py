import os
from afs2datasource import DBManager, constant

db_type = constant.DB_TYPE['AZUREBLOB']
account_name = ''
account_key = ''

container_name = 'stacytest'
source = 'titanic.csv'
folder_name = 'test'
destination = os.path.join(folder_name, source)
download_file = source
query = {
  'container': container_name,
  'blobs': {
    'files': [source],
    'folders': folder_name
  }
}

manager = DBManager(db_type=db_type,
  account_name=account_name,
  account_key=account_key,
  containers=query
)

try:
  is_success = manager.connect()
  print('Connection: {}'.format(is_success))

  is_table_exist = manager.is_table_exist(container_name)

  if not is_table_exist:
    print('Create Container {0} successfully: {1}'.format(container_name, manager.create_table(container_name)))
  print('Container {0} exist: {1}'.format(container_name, manager.is_table_exist(container_name)))

  is_file_exist = manager.is_file_exist(container_name, source)
  if not is_file_exist:
    manager.insert(table_name=container_name, source=source, destination=source)
    print('Insert file {0} successfully: {1}'.format(source, manager.is_file_exist(container_name, source)))
  print('File {0} is exist: {1}'.format(source, is_file_exist))

  is_file_exist = manager.is_file_exist(container_name, destination)
  if not is_file_exist:
    manager.insert(table_name=container_name, source=source, destination=destination)
    print('Insert file {0} successfully: {1}'.format(source, manager.is_file_exist(container_name, destination)))
  print('File {0} is exist: {1}'.format(destination, is_file_exist))

  if is_file_exist:
    response = manager.execute_query()
    print('Execute query successfully: {}'.format(response))

except Exception as e:
  print(e)
