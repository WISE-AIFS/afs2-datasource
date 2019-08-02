import os
from afs2datasource import DBManager, constant

# --------- config --------- #
db_type = constant.DB_TYPE['AZUREBLOB']
account_name = '' # azure blob account name
account_key = ''  # azure blob account key

container_name = '' # conatiner name
source = '' # 要上傳的檔案(local)
folder_name = ''  # 上傳上去的folder name

# 會上傳 source 到azure blob container裡面
# 一個在 /
# 一個在 /folder_name 下面
# 接著會下載這兩個檔案到local
# -------------------------- #

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
  # connect to azure blob
  is_success = manager.connect()
  print('Connection: {}'.format(is_success))

  # check if container is exist
  is_table_exist = manager.is_table_exist(container_name)

  # create container
  if not is_table_exist:
    print('Create Container {0} successfully: {1}'.format(container_name, manager.create_table(container_name)))
  print('Container {0} exist: {1}'.format(container_name, manager.is_table_exist(container_name)))

  # insert file
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

  # download files
  if is_file_exist:
    response = manager.execute_query()
    print('Execute query successfully: {}'.format(response))

except Exception as e:
  print(e)
