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
import asyncio
import logging
import pandas as pd
import afs2datasource.utils as utils
from azure.storage.blob import BlobServiceClient

# disable azure blob sdk logger
logging.getLogger('azure.storage').setLevel(logging.CRITICAL)

TOTAL_FILES_COUNT = 0


class AzureBlobHelper():
    def __init__(self, dataDir):
        self._connection = None
        data = utils.get_data_from_dataDir(dataDir)
        self.account_name, self.account_key = utils.get_azure_blob_credential(
            data)

    async def connect(self):
        if self._connection is None:
            connection = BlobServiceClient(
                account_url="https://{}.blob.core.windows.net".format(self.account_name),
                credential={
                    "account_name": self.account_name,
                    "account_key": self.account_key
                }
            )
            connection.list_containers()
            self._connection = connection
    
    def disconnect(self):
        self._connection = None

    async def execute_query(self, query_list):
        global TOTAL_FILE_COUNT
        TOTAL_FILE_COUNT = 0
        query_list = self._generate_download_list(query_list)
        print("\n-------START DOWNLOAD FILES-------")
        await asyncio.gather(*[self._download_file(query) for query in query_list])
        if len(query_list) == 1 and query_list[0]['file'].endswith('.csv'):
            try:
                file_path = os.path.join(
                    query_list[0]['container'], query_list[0]['file'])
                df = pd.read_csv(file_path)
                return df
            except Exception as e:
                pass
        return list(set([query['container'] for query in query_list]))

    def _generate_download_list(self, query_list):
        global TOTAL_FILE_COUNT
        response = []
        for query in query_list:
            blob = query['blobs']
            container_name = query['container']
            client = self._connection.get_container_client(container_name)
            if 'files' in blob:
                if not isinstance(blob['files'], (list, )):
                    if not blob['files']:
                        blob['files'] = []
                    else:
                        blob['files'] = [blob['files']]
                response += list(
                    map(lambda file: {'container': container_name, 'file': file}, blob['files']))
            if 'folders' in blob:
                if not isinstance(blob['folders'], (list, )):
                    if not blob['folders']:
                        blob['folders'] = []
                    else:
                        blob['folders'] = [blob['folders']]
                for folder in blob['folders']:
                    try:
                        blobs = client.list_blobs(name_starts_with=folder)
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

            client = self._connection.get_blob_client(file['container'], file['file'])
            if not client.exists():
                return
            with open(file_path, 'wb') as download_file:
                download_file.write(client.download_blob().readall())
            
            TOTAL_FILE_COUNT += 1
            print("Already downloaded files: {}".format(
                TOTAL_FILE_COUNT), end='\r')
        except Exception as e:
            print("\nDownload file {0} fail: {1}".format(file['file'], e))

    def _is_table_name_invalid(self, table_name):
        """
        Bucket names must be at least 3 and no more than 63 characters long.
        Bucket names must not contain uppercase characters or underscores.
        Bucket names must start with a lowercase letter or number.
        """

        reg = "^[a-z0-9-]{3,63}$"
        return re.match(reg, table_name)

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
            client = self._connection.get_container_client(table_name)
            return client.exists()
        except Exception as e:
            error = ''
            try:
                if e.response.reason:
                    error = e.response.reason
            except:
                error = str(e)
            raise Exception(error)

    def is_file_exist(self, table_name, file_name):
        raise NotImplementedError()
        try:
            client = self._connection.get_blob_client(table_name, file_name)
            return client.exists()
        except Exception as e:
            if int(e.status_code) == 404:
                return False
            else:
                raise Exception(e.error_code)

    def create_table(self, table_name, columns):
        if not self._is_table_name_invalid(table_name):
            raise ValueError(
                'table_name / bucket_name is invalid. Use only lowercase letters and numbers, and at least 3 and no more than 63 characters long.')
        try:
            self._connection.create_container(table_name)
        except Exception as e:
            raise Exception(e.error_code)

    def insert(self, table_name, source, destination):
        try:
            client = self._connection.get_blob_client(table_name, destination)
            
            with open(source, 'rb') as data:
                client.upload_blob(data)
        except FileNotFoundError as e:
            raise e
        except Exception as e:
            raise Exception(e.error_code)

    async def delete_table(self, table_name):
        try:
            self._connection.delete_container(table_name)
        except Exception as e:
            raise Exception(e.error_code)

    def delete_record(self, table_name, file_name):
        client = self._connection.get_blob_client(table_name, file_name)
        if not client.exists():
            return
        client.delete_blob()
