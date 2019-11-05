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
import asyncio
import logging
import concurrent
import afs2datasource.utils as utils
from azure.storage.blob import BlockBlobService
from azure.common import AzureHttpError

# disable azure blob sdk logger
logging.getLogger('azure.storage').setLevel(logging.CRITICAL)

BLOBNOTFOUND = 'BlobNotFound'

class azureBlobHelper():
  def __init__(self):
    self._connection = None

  async def connect(self):
    data = utils.get_data_from_dataDir()
    account_name, account_key = utils.get_azure_blob_credential(data)
    if self._connection is None:
      connection = BlockBlobService(account_name=account_name, account_key=account_key)
      connection.list_containers()
      self._connection = connection
  
  def disconnect(self):
    self._connection = None
  
  async def execute_query(self, query_list):
    query_list = self._generate_download_list(query_list)
    await asyncio.gather(*[self._download_file(query) for query in query_list])
    return list(set([query['container'] for query in query_list]))

  def _generate_download_list(self, query_list):
    response = []
    for query in query_list:
      blob = query['blobs']
      container_name = query['container']
      if 'files' in blob:
        if not isinstance(blob['files'], (list, )):
          if not blob['files']:
            blob['files'] = []
          else:
            blob['files'] = [blob['files']]
        response += list(map(lambda file: {'container': container_name, 'file': file}, blob['files']))
      if 'folders' in blob:
        if not isinstance(blob['folders'], (list, )):
          if not blob['folders']:
            blob['folders'] = []
          else:
            blob['folders'] = [blob['folders']]
        for folder in blob['folders']:
          try:
            blobs = self._connection.list_blobs(container_name=container_name, prefix=folder)
          except Exception as e:
            raise Exception(e.error_code)
          response += list(map(lambda file: {'container': container_name, 'file': file.name}, blobs))
    return response

  async def _download_file(self, file):
    try:
      file_path = os.path.join(file['container'], file['file'])
      if not os.path.exists(file['container']):
        os.makedirs(file['container'])
      if '/' in file_path:  # dir
        folder, _ = os.path.split('{}'.format(file_path))
        if not os.path.exists(folder) or (os.path.exists(folder) and not os.path.isdir(folder)):
          os.makedirs(folder)
      self._connection.get_blob_to_path(
        container_name=file['container'],
        blob_name=file['file'],
        file_path=file_path
      )
    except AzureHttpError as e:
      if e.error_code == BLOBNOTFOUND:
        raise Exception('File: {} is not exist'.format(file['file']))
      else:
        raise Exception(e.error_code)
    except Exception as e:
        raise Exception(e)

  def check_query(self, query_list):
    if not isinstance(query_list, (list, )):
      query_list = [query_list]
    if len(query_list) == 0:
      raise ValueError('blobList is invalid')
    for query in query_list:
      if 'container' not in query:
        raise ValueError('container is necessary')
      if 'blobs' not in query:
        raise ValueError('blobs is necessary')
    return query_list

  def is_table_exist(self, table_name):
    # table_name is container name
    try:
      container_list = [container.name for container in self._connection.list_containers()]
      return table_name in container_list
    except Exception as e:
      raise Exception(e.error_code)

  def is_file_exist(self, table_name, file_name):
    try:
      response = self._connection.get_block_list(
        container_name=table_name,
        blob_name=file_name
      )
      return True
    except Exception as e:
      if int(e.status_code) == 404:
        return False
      else:
        raise Exception(e.error_code)

  def create_table(self, table_name, columns):
    try:
      self._connection.create_container(table_name)
    except Exception as e:
      raise Exception(e.error_code)

  def insert(self, table_name, source, destination):
    try:
      response = self._connection.create_blob_from_path(
        container_name=table_name,
        blob_name=destination,
        file_path=source
      )
    except Exception as e:
      raise Exception(e.error_code)

  async def delete_table(self, table_name):
    try:
      self._connection.delete_container(table_name)
    except Exception as e:
      raise Exception(e.error_code)
  
  def delete_record(self, table_name, file_name):
    self._connection.delete_blob(table_name, file_name)
