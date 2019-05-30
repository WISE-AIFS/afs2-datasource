import os
import json
import afs2datasource.constant as const
import afs2datasource.utils as utils
import botocore.session
from botocore.client import Config
import pandas as pd

class s3Helper():
  def __init__(self):
    self._connection = None
    self._bucket = ''

  def connect(self):
    data = utils.get_data_from_dataDir()
    end_point, access_key, secret_key, bucket = utils.get_s3_credential(data)
    if self._connection is None:
      config = Config(signature_version='s3')
      session = botocore.session.get_session()
      connection = session.create_client('s3',
        endpoint_url=end_point,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=config
      )
      self._connection = connection
      self._bucket = bucket
  
  def disconnect(self):
    raise NotImplementedError('[S3 datasource] disconnect not implemented.')
  
  def execute_query(self, querySql):
  
  def check_query(self, querySql):
    if type(querySql) is not str:
      raise ValueError('querySql is invalid')
    return querySql

  def is_table_exist(self, table_name):
    # table_name is file with path
    try:
      obj = connection.get_object(Bucket=self._bucket, Key=table_name)
      return True
    except:
      return False

  def create_table(self, table_name, columns):

  def insert(self, table_name, columns, records):
