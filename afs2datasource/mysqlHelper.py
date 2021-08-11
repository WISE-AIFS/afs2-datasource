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

import pandas as pd
import mysql.connector
from afs2datasource.utils import get_credential_from_dataDir
from afs2datasource.helper import Helper

TOTAL_FILES_COUNT = 0
TOTAL_DOWNLOAD_FILES = 0


class mysqlHelper(Helper):
    def __init__(self, credential):
        self._connection = None
        self._username, self._password, self._host, self._port, self._database = get_credential_from_dataDir(credential)

    async def connect(self):
        if self._connection is None:
            connection = mysql.connector.connect(
                user=self._username,
                password=self._password,
                host=self._host,
                port=self._port,
                database=self._database,
            )
            if not connection.is_connected():
                raise ConnectionError('Failed to connect MySQL: {}:{}, username: {}, password: {}, database: {}'.format(
                    self._host, self._port, self._username, self._password, self._database))
            self._connection = connection

    def disconnect(self):
        if self._connection and self._connection.is_connected():
            self._connection.close()
        self._connection = None

    async def execute_query(self, querySql):
        cursor = self._connection.cursor()
        cursor.execute(querySql)
        columns = list(cursor.column_names)
        data = list(cursor.fetchall())
        data = pd.DataFrame(data=data, columns=columns)
        return data

    def check_query(self, querySql):
        if type(querySql) is not str:
            raise ValueError('querySql is invalid')
        return querySql

    def is_table_exist(self, table_name):
        cursor = self._connection.cursor()
        cursor.execute('SHOW TABLES')
        tables = [table[0] for table in cursor]
        return table_name in tables

    def is_file_exist(self, **kwargs):
        raise NotImplementedError('MySQL not implement.')

    def create_table(self, table_name, columns):
        command = 'CREATE TABLE {table} ('.format(table=table_name)
        fields = []
        for col in columns:
            field = '{name} {type}'.format(name=col['name'], type=col['type'])
            if col['is_primary']:
                field += ' PRIMARY KEY'
            if col['is_not_null']:
                field += ' NOT NULL'
            fields.append(field)
        command += ','.join(fields) + ')'
        cursor = self._connection.cursor()
        cursor.execute(command)

    def insert(self, table_name, columns, records):
        # cursor = self._connection.cursor()
        for record in records:
            if len(record) != len(columns):
                raise IndexError('record {} and columns do not match'.format(record))
        records = [tuple(record) for record in records]
        command = 'INSERT INTO {table_name} ('.format(table_name=table_name)
        command += ', '.join(columns) + ') VALUES ('
        command += ', '.join(['%s' for _ in range(len(columns))]) + ')'
        cursor = self._connection.cursor()
        cursor.executemany(command, records)
        self._connection.commit()

    async def delete_table(self, table_name):
        cursor = self._connection.cursor()
        command = 'DROP TABLE IF EXISTS {table_name}'.format(table_name=table_name)
        cursor.execute(command)
        self._connection.commit()

    def delete_record(self, table_name, condition):
        cursor = self._connection.cursor()
        command = 'DELETE FROM {table_name} WHERE {condition}'.format(table_name=table_name, condition=condition)
        cursor.execute(command)
        self._connection.commit()
