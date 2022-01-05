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
import boto3
import asyncio
import logging
import pandas as pd
from  afs2datasource.utils import get_s3_credential, is_table_name_invalid
from afs2datasource.constant import DB_TYPE
from afs2datasource.helper import Helper
from botocore.utils import is_valid_endpoint_url
from botocore.client import Config
from botocore.exceptions import ClientError

TOTAL_FILES_COUNT = 0
TOTAL_DOWNLOAD_FILES = 0


class s3Helper(Helper):
    def __init__(self, credential, type):
        self._connection = None
        self._type = type
        self.endpoint, self.access_key, self.secret_key, self.is_verify = get_s3_credential(credential)

        if type == DB_TYPE['S3']:
            if not self.endpoint:
                raise AttributeError('No endpoint in credential')
            if not is_valid_endpoint_url(self.endpoint):
                raise ValueError('Invalid endpoint: {}'.format(self.endpoint))

    async def connect(self):
        if self._connection is None:
            self._connection = self._get_connection()

    def disconnect(self):
        self._connection = None

    async def execute_query(self, query_list):
        global TOTAL_FILE_COUNT, TOTAL_DOWNLOAD_FILES
        TOTAL_FILE_COUNT = 0
        response = await asyncio.gather(*[self._generate_download_list(query) for query in query_list])
        file_list = []
        for res in response:
            file_list += res
        TOTAL_DOWNLOAD_FILES = 0
        print("\n-------START DOWNLOAD FILES-------")
        await asyncio.gather(*[self._download_file(file) for file in file_list])
        if len(file_list) == 1 and file_list[0]['file'].endswith('.csv'):
            try:
                file_path = os.path.join(
                    file_list[0]['bucket'], file_list[0]['file'])
                df = pd.read_csv(file_path)
                return df
            except Exception as e:
                pass

        buckets = list(set([file_obj['bucket'] for file_obj in file_list]))
        resp = list(filter(lambda query: query in buckets, list(map(lambda query: query['bucket'], query_list))))
        return resp

    async def _download_file(self, file):
        global TOTAL_FILES_COUNT, TOTAL_DOWNLOAD_FILES
        try:
            file_path = os.path.join(file['bucket'], file['file'])
            if not os.path.exists(file['bucket']):
                os.makedirs(file['bucket'])
            if '/' in file_path:  # dir
                folder, _ = os.path.split('{}'.format(file_path))
                if not os.path.exists(folder) or (os.path.exists(folder) and not os.path.isdir(folder)):
                    os.makedirs(folder)
            self._connection.download_file(
                file['bucket'], file['file'], file_path)
            TOTAL_DOWNLOAD_FILES += 1
            print("Already download files: {0}/{1}".format(
                TOTAL_DOWNLOAD_FILES, TOTAL_FILE_COUNT), end='\r')
        # except ClientError as e:
        #   raise Exception('{0}: {1}'.format(e.response['Error']['Code'], file['file']))
        except Exception as e:
            print("\nDownload file {0} fail: {1}".format(file['file'], e))

    def check_query(self, query_list):
        if not isinstance(query_list, (list, )):
            query_list = [query_list]
        if len(query_list) == 0:
            raise ValueError('buckets is invalid')
        for query in query_list:
            if 'bucket' not in query:
                raise ValueError('bucket is necessary')
            if 'blobs' not in query:
                raise ValueError('blobs is necessary')
        return query_list

    def is_table_exist(self, table_name):
        try:
            bucket_list = [bucket['Name']
                           for bucket in self._connection.list_buckets()['Buckets']]
            return table_name in bucket_list
        except TypeError as e:
            raise Exception(e)
        except Exception as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise Exception(e.response['Error']['Message'])

    def is_file_exist(self, table_name, file_name):
        try:
            _ = self._connection.head_object(
                Bucket=table_name,
                Key=file_name
            )
            return True
        except Exception as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise Exception(e.response['Error']['Message'])

    def create_table(self, table_name, **kwargs):
        region = kwargs.get('region', 'us-west-1')
        if not is_table_name_invalid(table_name):
            raise ValueError(
                'table_name / bucket_name is invalid. Use only lowercase letters and numbers, and at least 3 and no more than 63 characters long.')

        if self._type == DB_TYPE['S3']:
            self._connection.create_bucket(Bucket=table_name)
        else:
            _connection = self._get_connection(region_name=region)
            location = {'LocationConstraint': region}
            _connection.create_bucket(Bucket=table_name, CreateBucketConfiguration=location)

    def insert(self, table_name, source, destination):
        try:
            with open(source, 'rb') as data:
                self._connection.upload_fileobj(
                    Fileobj=data, Bucket=table_name, Key=destination)
        except FileNotFoundError:
            raise Exception('FileNotFound: {}'.format(source))
        except Exception as e:
            raise Exception(e.response['Error']['Message'])

    async def delete_table(self, table_name):
        try:
            resp = self._connection.list_objects(Bucket=table_name)
            files = []
            if 'Contents' in resp:
                for obj in resp['Contents']:
                    files.append(obj['Key'])

            await asyncio.gather(*[self._delete_file(table_name, file_name) for file_name in files])
            self._connection.delete_bucket(Bucket=table_name)
        except ClientError as e:
            raise Exception(e.args)
        except Exception as e:
            raise Exception(e.response['Error']['Message'])

    def delete_record(self, table_name, file_name):
        self._connection.delete_object(
            Bucket=table_name,
            Key=file_name
        )

    async def _generate_download_list(self, query):
        global TOTAL_FILE_COUNT
        response = []
        bucket_name = query['bucket']
        blob = query['blobs']
        if 'files' in blob:
            if not isinstance(blob['files'], (list, )):
                if not blob['files']:
                    blob['files'] = []
                else:
                    blob['files'] = [blob['files']]
            blob['files'] = list(filter(None, blob['files']))
            TOTAL_FILE_COUNT += len(blob['files'])
            print("Counting the download files: {}".format(
                TOTAL_FILE_COUNT), end='\r')
            response += list(
                map(lambda file: {'bucket': bucket_name, 'file': file}, blob['files']))
        if 'folders' in blob:
            if not isinstance(blob['folders'], (list, )):
                if not blob['folders']:
                    blob['folders'] = []
                else:
                    blob['folders'] = [blob['folders']]
            blob['folders'] = list(filter(None, blob['folders']))
            paginator = self._connection.get_paginator('list_objects')
            for folder in blob['folders']:
                try:
                    if not folder.endswith('/'):
                        folder = '{}/'.format(folder)
                    operation_parameters = {
                        'Bucket': bucket_name,
                        'Prefix': folder
                    }
                    page_iterator = paginator.paginate(**operation_parameters)
                    for page in page_iterator:
                        if 'Contents' in page:
                            files = list(
                                filter(lambda obj: not obj['Key'].endswith('/'), page['Contents']))
                            TOTAL_FILE_COUNT += len(files)
                            print("Counting the download files: {}".format(
                                TOTAL_FILE_COUNT), end='\r')
                            response += list(
                                map(lambda obj: {'bucket': bucket_name, 'file': obj['Key']}, files))
                except Exception as e:
                    Exception(e.response['Error']['Message'])
        return response

    async def _delete_file(self, table_name, file_name):
        self._connection.delete_object(
            Bucket=table_name,
            Key=file_name
        )

    def _get_connection(self, region_name=None):
        if self._type == DB_TYPE['S3']:
            connection = boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url=self.endpoint,
                config=Config(signature_version='s3'),
                verify=self.is_verify
            )
        else:
            if region_name:
                connection = boto3.client(
                    's3',
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
                    region_name=region_name,
                )
            else:
                connection = boto3.client(
                    's3',
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
                )
        connection.list_buckets()['Buckets']    
        return connection
