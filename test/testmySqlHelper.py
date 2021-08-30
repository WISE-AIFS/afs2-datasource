import unittest
import asyncio
import os
from afs2datasource.mysqlHelper import mysqlHelper
import pandas.testing as pd_testing

## Definition ##
username: str = ''
password: str = ''
host: str = ''
port: int = 3306
database: str = ''
table_name: str = 'titanic'
not_exist_table_name: str = 'noexist'
filename = 'test/TestData.csv'

import mysql.connector
import pandas

def get_connection(user, password, host, port, database) -> mysql.connector:
    connection = mysql.connector.connect(
        user=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    if not connection.is_connected():
        raise ConnectionError('Failed to connect MySQL: {}:{}, username: {}, password: {}, database: {}'.format(
            host, port, username, password, database))

    return connection


def prepareUnitTest():
    ## Check Credential connect successfully
    connection = get_connection(
        user=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )

    ## Insert Test Data
    cursor = connection.cursor()
    cursor.execute("""SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{}'""".format(table_name))

    ### If Test Table is exist
    if cursor.fetchone()[0]:
        cursor.execute("DROP TABLE {}".format(table_name))

    ### Create Test Table
    cursor.execute("""CREATE TABLE {} (
        passenger_id INTEGER,
        survived INTEGER,
        pclass INTEGER,
        name VARCHAR(255),
        sex VARCHAR(255),
        age INTEGER,
        sib_sp INTEGER,
        parch INTEGER,
        ticket VARCHAR(255),
        fare FLOAT,
        cabin VARCHAR(255),
        embarked VARCHAR(255))
    """.format(table_name))

    ### Insert Test Data
    df = pandas.read_csv(filename)
    sql = """INSERT INTO {} (
        passenger_id,
        survived,
        pclass,
        name,
        sex,
        age,
        sib_sp,
        parch,
        ticket,
        fare,
        cabin,
        embarked
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""".format(table_name)

    df = df.where(pandas.notnull(df), None)
    val = [tuple(row) for _, row in df.iterrows()]
    cursor.executemany(sql, val)
    connection.commit()

    ## Drop NotExist Test Table
    cursor = connection.cursor()
    cursor.execute("""SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{}'""".format(not_exist_table_name))

    ### If Test Table is exist
    if cursor.fetchone()[0]:
        cursor.execute("DROP TABLE {}".format(not_exist_table_name))

    cursor.close()

class mySQLUserTestCase(unittest.TestCase):
    querySql = 'SELECT * from {}'.format(table_name)
    def setUp(self):
        self.helper = mysqlHelper({
            'credential': {
                'username': username,
                'password': password,
                'host': host,
                'port': port,
                'database': database
            }
        })

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.helper.connect())

        self.connection = get_connection(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )

    def tearDown(self):
        self.helper.disconnect()
        self.helper = None

    def testExecuteQueryWithParams(self):
        loop = asyncio.get_event_loop()
        resp = loop.run_until_complete(self.helper.execute_query(self.__class__.querySql))
        df = pandas.read_csv(filename)

        self.assertIs(type(resp), pandas.DataFrame)
        pd_testing.assert_frame_equal(resp, df)

    def testCheckQuery(self):
        resp = self.helper.check_query(self.__class__.querySql)

        self.assertEqual(self.__class__.querySql, resp)

    def testIsTableExist(self):
        resp = self.helper.is_table_exist(table_name)
        self.assertTrue(resp)

        resp = self.helper.is_table_exist(not_exist_table_name)
        self.assertFalse(resp)

    def testIsFileExist(self):
        self.assertRaises(NotImplementedError, self.helper.is_file_exist)
        
    def testCreateTable(self):
        name = 'unittest'
        columns = [{
            'name': 'name',
            'type': 'VARCHAR(255)',
            'is_primary': False,
            'is_not_null': False
        }]

        self.helper.create_table(name, columns)

        cursor = self.connection.cursor()
        cursor.execute("""SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{}'""".format(name))

        self.assertEqual(1, cursor.fetchone()[0])

        cursor.execute("DROP TABLE {}".format(name))
        self.connection.commit()
        cursor.close()

    def testInsert(self):
        name = 'unittest1'
        columns = ['name']
        records = [{'name': 'stacy'}]
 
        cursor = self.connection.cursor()
        cursor.execute("""CREATE TABLE {} (
            name VARCHAR(255))
        """.format(name))

        self.helper.insert(name, columns, records)

        cursor.execute("""SELECT * FROM {}""".format(name))
        self.assertEqual(1, len(list(cursor.fetchall())))

        cursor.execute("DROP TABLE {}".format(name))
        self.connection.commit()
        cursor.close()

    def testDeleteTable(self):
        name = 'unittest2'
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.helper.delete_table(name))

        cursor = self.connection.cursor()
        cursor.execute("""SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{}'""".format(name))
        self.assertEqual(0, cursor.fetchone()[0])

        cursor.execute("""CREATE TABLE {} (
            name VARCHAR(255))
        """.format(name))

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.helper.delete_table(name))

        cursor.execute("""SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{}'""".format(name))
        self.assertEqual(0, cursor.fetchone()[0])

    def testDeleteRecord(self):
        name = 'unittest3'
        cursor = self.connection.cursor()
        cursor.execute("""CREATE TABLE {} (
            name VARCHAR(255),
            age INTEGER)
        """.format(name))

        sql = """INSERT INTO {} (name, age) VALUES (%s, %s)""".format(name)
        val = [('John', 5), ('test', 10)]
        cursor.executemany(sql, val)
        self.connection.commit()

        condition = 'name="{}"'.format('John')
        self.helper.delete_record(name, condition)

        cursor.execute("""SELECT * FROM {} WHERE name='{}'""".format(name, 'John'))
        self.assertEqual(0, len(list(cursor.fetchall())))

        cursor.execute("DROP TABLE {}".format(name))
        self.connection.commit()    

if __name__ == '__main__':
    prepareUnitTest()
    unittest.main()