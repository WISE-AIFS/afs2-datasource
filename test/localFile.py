from afs2datasource import DBManager, constant

print('-----POSTGRES-----')
# postgres
db_type = constant.DB_TYPE['POSTGRES']
username = 'c8b60369-5832-4867-bb26-084292943ae4'
password = '1stka6i60dpit8ei4496d25keh'
host = '124.9.14.58'
port = '5432'
database = '6fc942d1-6739-4fe1-b714-d737beebc707'
querySql = 'select * from test.data'
collection = None

db = DBManager(db_type=db_type,
username=username,
password=password,
host=host,
port=port,
database=database,
querySql=querySql)
db.connect()
print(db.execute_query())

print('-----MONGODB-----')
# mongo
db_type = constant.DB_TYPE['MONGODB']
username = '4b514b83-03ec-4a7c-9397-d5511c9eb1ec'
password = 'uUS6JZ3MK87r54u8syQSKPm2t'
host = '124.9.14.60'
port = 27017
database = 'a7fa213f-c35d-4c3a-bd00-0c8387c02384'
querySql = '{}'
collection='test'

db = DBManager(db_type=db_type,
username=username,
password=password,
host=host,
port=port,
database=database,
collection=collection,
querySql=querySql)
db.connect()
print(db.execute_query())

print('-----INFLUX-----')
# influx
db_type = constant.DB_TYPE['INFLUXDB']
username = 'a79d2e7b-3819-4910-b1e5-34d131b303dc'
password = 'el3orpWao6C4tbL5QKr7XBblQ'
host = '124.9.14.84'
port = '8086'
database = '5c510960-48e3-459e-8303-bfa8a46e8da1'
querySql = 'SELECT * FROM data_mea_1'

db = DBManager(db_type=db_type,
username=username,
password=password,
host=host,
port=port,
database=database,
querySql=querySql)
db.connect()
print(db.execute_query())