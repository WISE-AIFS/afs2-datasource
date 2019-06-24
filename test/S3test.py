import unittest
from afs2datasource import DBManager, constant

class TestS3Hepler(unittest.TestCase):
  def setUp(self):
    self.db_type = constant.DB_TYPE['S3']
    self.endpoint = 'http://61.218.118.232:8080'
    self.access_key = 'e75abc88cd974d46b5b467ac0dec1282'
    self.secret_key = 'ZYpL67CXMAaLchp0tKByF3XUmpgfZzhEi'
    self.bucket_name = 'test'
    self.manager = DBManager(
      db_type=self.db_type,
      endpoint=self.endpoint,
      access_key=self.access_key,
      secret_key=self.secret_key,
      bucket_name=self.bucket_name
    )
    self.manager.connect()

  def test_insert_and_delete_file(self):
    bucket_name = self.bucket_name
    source = '../README.md'
    destination = 'README.md'

    reseponse = self.manager.insert(table_name=bucket_name, source=source, destination=destination)
    self.assertTrue(reseponse)
    self.assertTrue(self.manager.is_file_exist(table_name=bucket_name, file_name=destination))
    
    reseponse = self.manager.delete_file(table_name=bucket_name, file_name=destination)
    self.assertTrue(reseponse)
    self.assertFalse(self.manager.is_file_exist(table_name=bucket_name, file_name=destination))
  
  def tearDown(self):
    self.manager = None

if __name__ == '__main__':
  unittest.main()