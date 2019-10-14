import unittest
from afs2datasource import DBManager, constant

class TestPostgresHelper(unittest.TestCase):
	def setUp(self):
		self.db_type = constant.DB_TYPE['POSTGRES']
		self.username = 'postgres'
		self.password = 'postgres'
		self.host = '172.16.8.84'
		self.port = 5432
		self.database = 'test'
		self.querySql = ''
		self.table_name = 'afs.titanic'
		self.columns = [{'name': 'passenger_id', 'type': 'INTEGER'}]
		self.manager = DBManager(
		db_type=self.db_type,
			username=self.username,
			password=self.password,
			host=self.host,
			port=self.port,
			database=self.database,
			querySql=self.querySql
		)
		self.manager.connect()

#	 def test_insert_and_delete_file(self):
#		 reseponse = self.manager.insert(table_name=self.bucket_name, source=self.source, destination=self.source)
#		 self.assertTrue(reseponse)
#		 self.assertTrue(self.manager.is_file_exist(table_name=self.bucket_name, file_name=self.source))
		
#		 reseponse = self.manager.delete_file(table_name=self.bucket_name, file_name=self.source)
#		 self.assertTrue(reseponse)
#		 self.assertFalse(self.manager.is_file_exist(table_name=self.bucket_name, file_name=self.source))
	
	def test_create_table_and_delete_table(self):
		resp = self.manager.is_table_exist(table_name=self.table_name)
		self.assertFalse(resp)

		resp = self.manager.create_table(table_name=self.table_name, columns=self.columns)
		self.assertTrue(resp)

		resp = self.manager.is_table_exist(table_name=self.table_name)
		self.assertTrue(resp)

		resp = self.manager.delete_table(table_name=self.table_name)
		self.assertTrue(resp)

		resp = self.manager.is_table_exist(table_name=self.table_name)
		self.assertFalse(resp)


	def tearDown(self):
		self.manager = None

if __name__ == '__main__':
	unittest.main()
