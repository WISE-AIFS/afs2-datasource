import unittest
from afs2datasource import DBManager, constant

class TestS3Hepler(unittest.TestCase):
  def setUp(self):
    self.db_type = constant.DB_TYPE['S3']
    self.endpoint = ''
    self.access_key = ''
    self.secret_key = ''
    self.bucket_name = 'test'
    self.source = 'titanic.csv'
    self.folder = 'test/'
    self.destination = '{}/{}'.format(self.folder, self.source)
    self.buckets = [{
      'bucket': self.bucket_name,
      'blobs': {
        'files':[self.source],
        'folders':[self.folder]
      }
    }]
    self.manager = DBManager(
      db_type=self.db_type,
      endpoint=self.endpoint,
      access_key=self.access_key,
      secret_key=self.secret_key,
      buckets=self.buckets
    )
    self.manager.connect()

  def test_insert_and_delete_file(self):

    reseponse = self.manager.insert(table_name=self.bucket_name, source=self.source, destination=self.source)
    self.assertTrue(reseponse)
    self.assertTrue(self.manager.is_file_exist(table_name=self.bucket_name, file_name=self.source))
    
    reseponse = self.manager.delete_file(table_name=self.bucket_name, file_name=self.source)
    self.assertTrue(reseponse)
    self.assertFalse(self.manager.is_file_exist(table_name=self.bucket_name, file_name=self.source))
  
  def tearDown(self):
    self.manager = None

if __name__ == '__main__':
  unittest.main()