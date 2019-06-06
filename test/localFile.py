from afs2datasource import DBManager, constant

print('-----POSTGRES-----')
# postgres
db_type = constant.DB_TYPE['POSTGRES']
username = ''
password = ''
host = ''
port = 
database = ''
querySql = ''
collection = None

db = DBManager(db_type=db_type,
username=username,
password=password,
host=host,
port=port,
database=database,
querySql=querySql)
db.connect()
try:
  db.execute_query()
  print(True)
except Exception as e:
  print(e)

print('-----MONGODB-----')
# mongo
db_type = constant.DB_TYPE['MONGODB']
username = ''
password = ''
host = ''
port = 
database = ''
querySql = '{}'
collection = ''

db = DBManager(db_type=db_type,
username=username,
password=password,
host=host,
port=port,
database=database,
collection=collection,
querySql=querySql)
db.connect()
try:
  db.execute_query()
  print(True)
except Exception as e:
  print(e)

print('-----INFLUX-----')
# influx
db_type = constant.DB_TYPE['INFLUXDB']
username = ''
password = ''
host = ''
port = ''
database = ''
querySql = ''

db = DBManager(db_type=db_type,
username=username,
password=password,
host=host,
port=port,
database=database,
querySql=querySql)
db.connect()
try:
  db.execute_query()
  print(True)
except Exception as e:
  print(e)

print('-----S3-----')
# influx
db_type = constant.DB_TYPE['S3']
end_point = ''
access_key = ''
secret_key = ''
bucket_name = ''
blob_list = ['']

db = DBManager(db_type=db_type,
end_point=end_point,
access_key=access_key,
secret_key=secret_key,
bucket_name=bucket_name,
database=database,
blob_list=blob_list)
db.connect()
try:
  db.execute_query()
  print(True)
except Exception as e:
  print(e)
