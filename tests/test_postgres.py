import unittest

from config import Config
from database import Database


class PostgresTestCase(unittest.TestCase):

    def setUp(self):
        self.config = Config('py-protodb.yaml')
        self.database = Database(self.config)

    @staticmethod
    def test_read_schema():
        print('hello')


if __name__ == '__main__':
    unittest.main()
