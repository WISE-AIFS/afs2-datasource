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
import logging
import afs2datasource.utils as utils
import boto3
from botocore.client import Config

class s3Helper():
  def __init__(self):
    self._connection = None
    self._bucket = ''

  def connect(self):
    try:
      data = utils.get_data_from_dataDir()
      end_point, access_key, secret_key, bucket = utils.get_s3_credential(data)
      if self._connection is None:
        config = Config(signature_version='s3')
        connection = boto3.client(
          's3',
          aws_access_key_id=access_key,
          aws_secret_access_key=secret_key,
          endpoint_url=end_point,
          config=config
        )
        self._connection = connection
        self._bucket = bucket
        return True
    except Exception as e:
      logging.error('[connect]: {}'.format(e))
      return False
  
  def disconnect(self):
    raise NotImplementedError('[S3 datasource] disconnect not implemented.')
  
  def execute_query(self, query_list):
    is_success = True
    for query_obj in query_list:
      if query_obj.endswith('/'): # dir
        try:
          response = self._connection.list_objects(
            Bucket=self._bucket,
            Prefix=query_obj
          )
          for obj in response['Contents'][::-1]:
            if obj['Key'] != query_obj:
              query_list.append(obj['Key'])
          if not os.path.exists(query_obj):
              os.makedirs(query_obj)
        except Exception as e:
          logging.error('[execute_query]: {}'.format(e))
          is_success = False
      else:
        split_list = query_obj.rsplit('/', 1)
        folder = split_list[0] if len(split_list) > 1 else ''
        filename = split_list[-1]
        try:
          if folder and not os.path.exists(folder):
            os.makedirs(folder)
          if os.path.exists(query_obj) and os.path.isdir(query_obj):
            logging.error('[execute_query]: file {} is exist'.format(filename))
            is_success = False
            continue
          obj = self._connection.get_object(Bucket=self._bucket, Key=query_obj)
          with open(query_obj, 'wb') as f:
            while f.write(obj['Body'].read()):
              pass
        except FileExistsError:
          logging.error('[execute_query]: file {} is exist'.format(filename))
          is_success = False
        except FileNotFoundError:
          logging.error('[execute_query]: No such file or directory {}'.format(query_obj))
          is_success = False
        except Exception as e:
          if 'response' in e and e.response['Error']['Code'] == 'NoSuchKey':
            logging.error('[execute_query]: file {} not exist'.format(query_obj))
          else:
            logging.error('[execute_query]: {}'.format(e))
          is_success = False
    return is_success
  
  def check_query(self, query_list):
    if not isinstance(query_list, (list, )) or len(query_list) == 0:
      raise ValueError('blobList is invalid')
    return query_list

  def is_table_exist(self, table_name):
    # table_name is bucket
    try:
      bucket_list = [bucket['Name'] for bucket in self._connection.list_buckets()['Buckets']]
      return table_name in bucket_list
    except Exception as e:
      logging.error('[Is Table Exist]: {}'.format(e))
      return False

  def create_table(self, table_name, columns):
    try:
      self._connection.create_bucket(Bucket=table_name)
    except Exception as e:
      logging.error('[Create Table]: {}'.format(e))
      return False
    return True

  def insert(self, table_name, source, destination):
    try:
      response = self._connection.head_object(
        Bucket=table_name,
        Key=destination
      )
      logging.error('[Insert]: file {} is exist'.format(destination))
      return False
    except Exception as e:
      if e.response['Error']['Code'] == '404':
        try:
          with open(source, 'rb') as data:
            self._connection.upload_fileobj(Fileobj=data, Bucket=table_name, Key=destination)
        except Exception as e:
          logging.error('[Insert]: {}'.format(e))
          return False
        return True
      else:
        logging.error('[Insert]: {}'.format(e))
