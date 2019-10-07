# AFS2-DataSource SDK
The AFS2-DataSource SDK package allows developers to easily access PostgreSQL, MongoDB, InfluxDB, S3 and APM.

## Installation
Support Python version 3.6 or later
```
pip install afs2-datasource
```

## Development
```
pip install -e .
```

## Notice
AFS2-DataSource SDK uses `asyncio` package, and Jupyter kernel is also using `asyncio` and running an event loop, but these loops can't be nested.
(https://github.com/jupyter/notebook/issues/3397)

If using AFS2-DataSource SDK in Jupyter Notebook, please add the following codes to resolve this issue:
```python
!pip install nest_asyncio
import nest_asyncio
nest_asyncio.apply()
```

## API
### DBManager
+ <a href="#init"><code>Init DBManager</code></a>
+ <a href="#connect"><code>DBManager.<b>connect()</b></code></a>
+ <a href="#disconnect"><code>DBManager.<b>disconnect()</b></code></a>
+ <a href="#is_connected"><code>DBManager.<b>is_connected()</b></code></a>
+ <a href="#is_connecting"><code>DBManager.<b>is_connecting()</b></code></a>
+ <a href="#get_dbtype"><code>DBManager.<b>get_dbtype()</b></code></a>
+ <a href="#execute_query"><code>DBManager.<b>execute_query()</b></code></a>
+ <a href="#create_table"><code>DBManager.<b>create_table(table_name, columns)</b></code></a>
+ <a href="#is_table_exist"><code>DBManager.<b>is_table_exist(table_name)</b></code></a>
+ <a href="#is_file_exist"><code>DBManager.<b>is_file_exist(table_name, file_name)</b></code></a>
+ <a href="#insert"><code>DBManager.<b>insert(table_name, columns, records, source, destination)</b></code></a>
+ <a href="#delete_table"><code>DBManager.<b>delete_table(table_name)</b></code></a>
+ <a href="#delete_file"><code>DBManager.<b>delete_file(table_name, file_name)</b></code></a>
----
<a name="init"></a>
#### Init DBManager
<!--##### With Enviroment Variable
Database config from environment variable.

Export database config on command line.
```base
export PAI_DATA_DIR="{"type": "mongo-firehose","data": {querySql": "{QUERY_STRING}","collection": "{COLLECTION_NAME}","credential": {"username": "{DB_USERNAME}","password": "{DB_PASSWORD}","database": "{DB_NAME}","port": {DB_PORT},"host": "{DB_HOST}"}}}"

export PAI_DATA_DIR="{"type": "s3-firehose", "data": {"bucketName": "{BUCKET_NAME}", "blobList": ["{FILE_NAME}"], "credential": {"endpoint\": "{ENDPOINT}", "accessKey": "{ACESSKEY}", "secretKey": "{SECRETKEY}"}}}"

```-->
##### With Database Config
Import database config via Python.
```python
from afs2datasource import DBManager, constant

# For PostgreSQL
manager = DBManager(db_type=constant.DB_TYPE['POSTGRES'],
  username=username,
  password=password,
  host=host,
  port=port,
  database=database,
  querySql="select {field} from {schema}.{table}"
)

# For MongoDB
manager = DBManager(db_type=constant.DB_TYPE['MONGODB'],
  username=username,
  password=password,
  host=host,
  port=port,
  database=database,
  collection=collection,
  querySql="{"{key}": {value}}"
)

# For InfluxDB
manager = DBManager(db_type=constant.DB_TYPE['INFLUXDB'],
  username=username,
  password=password,
  host=host,
  port=port,
  database=database,
  querySql="select {field_key} from {measurement_name}"
)

# For S3
manager = DBManager(db_type=constant.DB_TYPE['S3'],
  endpoint=endpoint,
  access_key=access_key,
  secret_key=secret_key,
  is_verify=False,
  buckets=[{
    'bucket': 'bucket_name',
    'blobs': {
      'files': ['file_name'],
      'folders': ['folder_name']
    }
  }]
)

# For APM
manager = DBManager(db_type=constant.DB_TYPE['APM'],
  username=username,  # sso username
  password=password,  # sso password
  apmUrl=apmUrl,
  machineIdList=[machineId],  # APM Machine Id
  parameterList=[parameter],  # APM Parameter
  mongouri=mongouri,
  # timeRange or timeLast
  timeRange=[{'start': start_ts, 'end': end_ts}],
  timeLast={'lastDays:' lastDay, 'lastHours': lastHour, 'lastMins': lastMin}
)

# For Azure Blob
manager = DBManager(db_type=constant.DB_TYPE['AZUREBLOB'],
  account_name=account_name,
  account_key=account_key,
  containers=[{
    'container': container_name,
    'blobs': {
      'files': ['file_name']
      'folders': ['folder_name']
    }
  }]
)
```
----
<a name="connect"></a>
#### DBManager.connect()
Connect to PostgreSQL, MongoDB, InfluxDB, S3, APM with specified by the given config.
```python
manager.connect()
```
----
<a name="disconnect"></a>
#### DBManager.disconnect()
Close the connection.
Note S3 datasource not support this function.
```python
manager.disconnect()
```
----
<a name="is_connected"></a>
#### DBManager.is_connected()
Return if the connection is connected.
```python
manager.is_connected()
```
----
<a name="is_connecting"></a>
#### DBManager.is_connecting()
Return if the connection is connecting.
```python
manager.is_connecting()
```
----
<a name="get_dbtype"></a>
#### DBManager.get_dbtype()
Return database type of the connection.
```python
manager.get_dbtype()
```
----
<a name="execute_query"></a>
#### DBManager.execute_query()
Return the result in PostgreSQL, MongoDB or InfluxDB after executing the `querySql` in config.

Download files which is specified in `buckets` in S3 config or `containers` in Azure Blob config, and return `buckets` and `containers` name of array.

Return data of `Machine` and `Parameter` in `timeRange` or `timeLast` from APM.

```python
# For Postgres, MongoDB, InfluxDB and APM
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

# For Azure Blob
container_names = manager.execute_query()
# Return Array
"""
['container1', 'container2']
"""

# For S3
bucket_names = manager.execute_query()
# Return Array
"""
['bucket1', 'bucket2']
"""

```
----
<a name="create_table"></a>
#### DBManager.create_table(table_name, columns=[])
Create table in database for Postgres, MongoDB and InfluxDB.
Noted, to create a new measurement in influxdb simply insert data into the measurement.

Create Bucket/Container in S3/Azure Blob.

Note: PostgreSQL table_name format **schema.table**
```python
# For Postgres, MongoDB and InfluxDB
table_name = 'titanic'
columns = [
  {'name': 'index', 'type': 'INTEGER', 'is_primary': True},
  {'name': 'survived', 'type': 'FLOAT', 'is_not_null': True},
  {'name': 'age', 'type': 'FLOAT'},
  {'name': 'embarked', 'type': 'INTEGER'}
]
manager.create_table(table_name=table_name, columns=columns)

# For S3
bucket_name = 'bucket'
manager.create_table(table_name=bucket_name)

# For Azure Blob
container_name = 'container'
manager.create_table(table_name=container_name)
```
----
<a name="is_table_exist"></a>
#### DBManager.is_table_exist(table_name)
Return if the table is exist in Postgres, MongoDB or Influxdb.

Return if the bucket is exist in S3.

Return if the container is exist in Azure Blob.

```python
# For Postgres, MongoDB and InfluxDB
table_name = 'titanic'
manager.is_table_exist(table_name=table_name)

# For S3
bucket_name = 'bucket'
manager.is_table_exist(table_name=bucket_name)

# For Azure blob
container_name = 'container'
manager.is_table_exist(table_name=container_name)
```
----
<a name="is_file_exist"></a>
#### DBManager.is_file_exist(table_name, file_name)
Return if the file is exist in bucket in S3.
Return if the file is exist in container in Azure Blob.

Note this function only support S3 and Azure Blob.
```python
# For S3
bucket_name = 'bucket'
file_name = 'test.csv
manager.is_file_exist(table_name=bucket_name, file_name=file_name)
# Return: Boolean

# For Azure Blob
container_name = 'container'
file_name = 'test.csv
manager.is_file_exist(table_name=container_name, file_name=file_name)
# Return: Boolean
```
----
<a name="insert"></a>
#### DBManager.insert(table_name, columns=[], records=[], source='', destination='')
Insert records into table in Postgres, MongoDB or InfluxDB.

Upload file to S3 and Azure Blob.

```python
# For Postgres, MongoDB and InfluxDB
table_name = 'titanic'
columns = ['index', 'survived', 'age', 'embarked']
records = [
  [0, 1, 22.0, 7.0],
  [1, 1, 2.0, 0.0],
  [2, 0, 26.0, 7.0]
]
manager.insert(table_name=table_name, columns=columns, records=records)

# For S3
bucket_name = 'bucket'
source='test.csv' # local file path
destination='test_s3.csv' # the file path and name in s3
manager.insert(table_name=bucket_name, source=source, destination=destination)

# For Azure Blob
container_name = 'container'
source='test.csv' # local file path
destination='test_s3.csv' # the file path and name in Azure blob
manager.insert(table_name=container_name, source=source, destination=destination)
```
---
#### Use APM data source
* Get Hist Raw data from SCADA Mongo data base
* Required
  - username: APM SSO username
  - password: APM SSO password
  - uri: mongo data base uri
  - apmurl: APM api url
  - machineIdList: APM machine Id list (**type:Array**)
  - parameterList: APM parameter name list (**type:Array**)
  - time range: Training date range
    * example:
    ```json
    [{'start':'2019-05-01', 'end':'2019-05-31'}]
    ```
----
<a name="delete_table"></a>
#### DBManager.delete_table(table_name)
Delete table in Postgres, MongoDB or InfluxDB, and return if the table is deleted successfully.

Delete the bucket in S3 and return if the table is deleted successfully.

Delete the container in Azure Blob and return if the table is deleted successfully.

```python
# For Postgres, MongoDB or InfluxDB
table_name = 'titanic'
is_success = manager.delete_table(table_name=table_name)
# Return: Boolean

# For S3
bucket_name = 'bucket'
is_success = manager.delete_table(table_name=bucket_name)
# Return: Boolean

# For Azure Blob
container_name = 'container'
is_success = manager.delete_table(table_name=container_name)
# Return: Boolean
```
----
<a name="delete_file"></a>
#### DBManager.delete_file(table_name, file_name)
Delete file in bucket in S3 and return if the file is deleted successfully.

Note this function only support S3.

```python
# For S3
bucket_name = 'bucket'
file_name = 'test_s3.csv'
manager.delete_file(table_name=bucket_name, file_name=file_name)
# Return: Boolean
```
---
# Example

## MongoDB Example

```python
from afs2datasource import DBManager, constant

# Init DBManager
manager = DBManager(
 db_type=constant.DB_TYPE['MONGODB'],
 username={USERNAME},
 password={PASSWORD},
 host={HOST},
 port={PORT},
 database={DATABASE},
 collection={COLLECTION},
 querySql={QUERYSQL}
)

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

# Delete Table
is_success = db.delete_table(table_name=table_name)
# Return type: Boolean

# Disconnect to DB
manager.disconnect()
```
---
## S3 Example

```python
from afs2datasource import DBManager, constant

# Init DBManager
manager = DBManager(
  db_type = constant.DB_TYPE['S3'],
  endpoint={ENDPOINT},
  access_key={ACCESSKEY},
  secret_key={SECRETKEY},
  buckets=[{
    'bucket': {BUCKET_NAME},
    'blobs': {
      'files': ['dataset/train.csv'],
      'folders': ['models/']
    }
  }]
)

# Connect S3
manager.connect()

# Check is the table is exist
bucket_name = 'titanic'
manager.is_table_exist(table_name=bucket_name)
# Return type: boolean

# Create Bucket
manager.create_table(table_name=bucket_name)

# Upload File to S3
local_file = '../test.csv'
s3_file = 'dataset/test.csv'
manager.insert(table_name=bucket_name, source=local_file, destination=s3_file)

# Download files in blob_list
# Download all files in directory
bucket_names = manager.execute_query()
# Return type: Array

# Check if file is exist or not
is_exist = manager.is_file_exist(table_name=bucket_name, file_name=s3_file)
# Return type: Boolean

# Delete the file in Bucket and return if the file is deleted successfully
is_success = manager.delete_file(table_name=bucket_name, file_name=s3_file)
# Return type: Boolean

# Delete Bucket
is_success = manager.delete_table(table_name=bucket_name)
# Return type: Boolean
```
---

## APM Data source example
```python
APMDSHelper(
  username,
  password,
  apmurl,
  machineIdList,
  parameterList,
  mongouri,
  timeRange)
APMDSHelper.execute()
```
---

## Azure Blob Example

```python
from afs2datasource import DBManager, constant

# Init DBManager
manager = DBManager(
 db_type=constant.DB_TYPE['AZUREBLOB'],
 account_key={ACCESS_KEY},
 account_name={ACCESS_NAME}
 containers=[{
   'container': {CONTAINER_NAME},
   'blobs': {
     'files': ['titanic.csv', 'models/train.csv'],
     'folders': ['test/']
   }
 }]
)

# Connect Azure Blob
manager.connect()

# Check is the container is exist
container_name = 'container'
manager.is_table_exist(table_name=container_name)
# Return type: boolean

# Create container
manager.create_table(table_name=container_name)

# Upload File to Azure Blob
local_file = '../test.csv'
azure_file = 'dataset/test.csv'
manager.insert(table_name=container_name, source=local_file, destination=azure_file)

# Download files in `containers`
# Download all files in directory
container_names = manager.execute_query()
# Return type: Array

# Check if file is exist in container or not
is_exist = manager.is_file_exist(table_name=container_name, file_name=azure_file)
# Return type: Boolean

# Delete Container
is_success = manager.delete_table(table_name=container_name)
# Return type: Boolean
```