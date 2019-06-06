DB_STATUS = {
  'DISCONNECTED': 0,
  'CONNECTED': 1,
  'CONNECTING': 2
}

DB_TYPE = {
  'MONGODB': 'mongo-firehose',
  'POSTGRES': 'postgresql-firehose',
  'INFLUXDB': 'influxdb-firehose',
  'S3': 's3-firehose'
}

DB_CONNECTION_TIMEOUT = 500 # ms