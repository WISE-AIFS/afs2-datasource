# AFS2-DataSource SDK
The AFS2-DataSource SDK package allows developers to easily access PostgreSQL, MongoDB, InfluxDB.

## Installation
Support Pyton version 3.6 or later
```
pip install afs2-datasource
```

## Example
### Database config
Database config from environment variable.

Export database config on command line.
```base
export PAI_DATA_DIR="{"type": "mongo-firehose","data": {"dbType": "internal","querySql": "{QUERY_STRING}","collection": "{COLLECTION_NAME}","credential": {"username": "{DB_USERNAME}","password": "{DB_PASSWORD}","database": "{DB_NAME}","port": {DB_PORT},"host": "{DB_HOST}"}}}"
```

Export database config via Python
```python
os.environ['PAI_DATA_DIR'] = """{
    "type": "mongo-firehose",
    "data": {
      "dbType": "internal",
      "querySql": "{QUERY_STRING}",
      "collection": "{COLLECTION_NAME}",
      "credential": {
        "username": "{DB_USERNAME}",
        "password": "{DB_PASSWORD}",
        "database": "{DB_NAME}",
        "port": {DB_PORT},
        "host": "{DB_HOST}"
      }
    }
  }
  """
```

### DBManager Example
```python
from afs2datasource import DBManager

# Init DBManager
manager = DBManager()

# Connect DB
manager.connect()

# Check the status of connection
is_connected = manager.is_connected()
# Return type: boolean

# Execute querySql in DB config
data = manager.execute_query()
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

# Check is the table is exist
table_name = 'titanic'
manager.is_table_exist(table_name)
# Return type: boolean

# Create Table
columns = [
  {'name': 'index', 'type': 'INTEGER', 'is_not_null': True},
  {'name': 'survived', 'type': 'FLOAT'},
  {'name': 'age', 'type': 'FLOAT'},
  {'name': 'embarked', 'type': 'INTEGER'},
  {'name': 'fare', 'type': 'FLOAT'},
  {'name': 'pclass', 'type': 'INTEGER'},
  {'name': 'sex', 'type': 'INTEGER'},
  {'name': 'title2', 'type': 'INTEGER'},
  {'name': 'ticket_info', 'type': 'INTEGER'},
  {'name': 'cabin', 'type': 'INTEGER'}
]
manager.create_table(table_name=table_name, columns=columns)

# Disconnect to DB
manager.disconnect()

```

## API List
+ connect()
+ disconnect()
+ is_connected()
+ is_connecting()
+ get_dbtype()
+ execute_query()
+ create_table(table_name, columns)
+ is_table_exist(table_name)
+ insert(table_name, columns, records)