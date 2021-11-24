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

DB_STATUS = {
  'DISCONNECTED': 0,
  'CONNECTED': 1,
  'CONNECTING': 2
}

DB_TYPE = {
  'MYSQL': 'mysql-firehose',
  'MONGODB': 'mongo-firehose',
  'POSTGRES': 'postgresql-firehose',
  'INFLUXDB': 'influxdb-firehose',
  'ORACLEDB': 'oracledb-firehose',
  'S3': 's3-firehose',
  'AZUREBLOB': 'azure-firehose',
  'AWS': 'aws-firehose',
  'APM': 'apm-firehose',
  'DATAHUB': 'datahub-firehose',
}
