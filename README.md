# AFS2-DataSource SDK
The AFS2-DataSource SDK package allows developers to easily access PostgreSQL, MongoDB, InfluxDB.

## Installation
Support Pyton version 3.6 or later
```
pip install afs2-datasource
```

## Example

```python
from afs2datasource import DBManager

# Init DBManager with enviroment variable
manager = DBManager()

# Connect DB
manager.connect()

# Check the status of connection
is_connected = manager.is_connected()
# Return type: boolean

# Check is the table is exist
table_name = 'titanic'
manager.is_table_exist(table_name)
# Return type: boolean

# Create Table
columns = [
  {'name': 'index', 'type': 'INTEGER', 'is_not_null': True},
  {'name': 'survived', 'type': 'INTEGER'},
  {'name': 'age', 'type': 'FLOAT'},
  {'name': 'embarked', 'type': 'INTEGER'}
]
manager.create_table(table_name=table_name, columns=columns)

# Insert Record
columns = ['index', 'survived', 'age', 'embarked']
records = [
  [0, 1, 22.0, 7.0],
  [1, 1, 2.0, 0.0],
  [2, 0, 26.0, 7.0]
]
manager.insert(table_name=table_name, columns=columns, records=records)

# Execute querySql in DB config
data = manager.execute_query()
# Return type: DataFrame
"""
      index  survived   age   embarked
0         0         1   22.0       7.0
1         1         1    2.0       0.0
2         2         0   26.0       7.0
...
"""


# Disconnect to DB
manager.disconnect()

```

## API
### DBManager
+ <a hred="#connect"><code>DBManager.<b>connect()</b></code></a>
+ <a hred="#disconnect"><code>DBManager.<b>disconnect()</b></code></a>
+ <a hred="#is_connected"><code>DBManager.<b>is_connected()</b></code></a>
+ <a hred="#is_connecting"><code>DBManager.<b>is_connecting()</b></code></a>
+ <a hred="#get_dbtype"><code>DBManager.<b>get_dbtype()</b></code></a>
+ <a hred="#execute_query"><code>DBManager.<b>execute_query()</b></code></a>
+ <a hred="#create_table"><code>DBManager.<b>create_table(table_name, columns)</b></code></a>
+ <a hred="#is_table_exist"><code>DBManager.<b>is_table_exist(table_name)</b></code></a>
+ <a hred="#insert"><code>DBManager.<b>insert(table_name, columns, records)</b></code></a>
----
#### Init DBManager
##### With Enviroment Variable
Database config from environment variable.

Export database config on command line.
```base
export PAI_DATA_DIR="{"type": "mongo-firehose","data": {"dbType": "internal","querySql": "{QUERY_STRING}","collection": "{COLLECTION_NAME}","credential": {"username": "{DB_USERNAME}","password": "{DB_PASSWORD}","database": "{DB_NAME}","port": {DB_PORT},"host": "{DB_HOST}"}}}"
```
##### With Database Config
Import database config via Python.
```python
manager = DBManager(db_type=constant.DB_TYPE['MONGODB'],
  username=username,
  password=password,
  host=host,
  port=port,
  database=database,
  collection=collection,
  querySql=querySql
)
```
----
<a name="#connect"></a>
#### DBManager.connect()
Connect to PostgreSQL, MongoDB, InfluxDB with specified by the given config.
```python
manager.connect()
```
----
<a name="#disconnect"></a>
#### DBManager.disconnect()
Close the connection.
```python
manager.disconnect()
```
----
<a name="#is_connected"></a>
#### DBManager.is_connected()
Return if the connection is connected.
```python
manager.is_connected()
```
----
<a name="#is_connecting"></a>
#### DBManager.is_connecting()
Return if the connection is connecting.
```python
manager.is_connecting()
```
----
<a name="#get_dbtype"></a>
#### DBManager.get_dbtype()
Return database type of the connection.
```python
manager.get_dbtype()
```
----
<a name="#execute_query"></a>
#### DBManager.execute_query()
Return the result after executing the querySql in config.
```python
df = manager.execute_query()
# Return type: DataFrame
"""
      Age  Cabin  Embarked      Fare  ...  Sex  Survived  Ticket_info  Title2
0    22.0    7.0       2.0    7.2500  ...  1.0       0.0          2.0     2.0
1    38.0    2.0       0.0   71.2833  ...  0.0       1.0         14.0     3.0
2    26.0    7.0       2.0    7.9250  ...  0.0       1.0         31.0     1.0
3    35.0    2.0       2.0   53.1000  ...  0.0       1.0         36.0     3.0
4    35.0    7.0       2.0    8.0500  ...  1.0       0.0         36.0     2.0
...
"""
```
----
<a name="#create_table"></a>
#### DBManager.create_table(table_name, columns=[])
Create table in database.

Note: PostgreSQL table_name format **schema.table**
```python
table_name = 'titanic'
columns = [
  {'name': 'index', 'type': 'INTEGER', 'is_primary': True},
  {'name': 'survived', 'type': 'FLOAT', 'is_not_null': True},
  {'name': 'age', 'type': 'FLOAT'},
  {'name': 'embarked', 'type': 'INTEGER'}
]
manager.create_table(table_name=table_name, columns=columns)
```
----
<a name="#is_table_exist"></a>
#### DBManager.is_table_exist(table_name)
Return if the table is exist in database.
```python
table_name = 'titanic'
manager.is_table_exist(table_name=table_name)
```
----
<a name="#insert"></a>
#### DBManager.insert(table_name, columns=[], records=[])
Insert records into table
```python
table_name = 'titanic'
columns = ['index', 'survived', 'age', 'embarked']
records = [
  [0, 1, 22.0, 7.0],
  [1, 1, 2.0, 0.0],
  [2, 0, 26.0, 7.0]
]
manager.insert(table_name=table_name, columns=columns, records=records)
```
---

###### Use APM data source
* Get Hist Raw data from SCADA Mongo data base
* Required
  - username: APM SSO username
  - password: APM SSO password
  - uri: mongo data base uri
  - apmurl: APM api url
  - machineIdList: APM machine Id list **type:Array**
  - parameterList: APM parameter name list **type:Array**
  - time range: Training date range
    * example:
    ```json
    [{'start':'2019-05-01', 'end':'2019-05-31'}]
    ```
* Execute example
```python
APMDSHelper(username,password,apmurl,machineIdList,parameterList,mongouri,timeRange)
APMDSHelper.execute()
```