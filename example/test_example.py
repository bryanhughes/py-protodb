import unittest
import uuid

import psycopg
from google import protobuf

from example.config import Config
from example.test_schema import user_db


class CRUDTestCase(unittest.TestCase):
    def setUp(self):
        self.config = Config('config/example.yaml')
        host = self.config.get_config()['database']['host']
        port = self.config.get_config()['database']['port']
        dbname = self.config.get_config()['database']['database']
        user = self.config.get_config()['database']['user']
        password = self.config.get_config()['database']['password']
        self.conn_str = f'host={host} port={port} dbname={dbname} user={user} password={password}'
        self.conn = psycopg.connect(self.conn_str)
        self.conn.adapters.register_dumper(protobuf.pyext._message.RepeatedScalarContainer,
                                           psycopg.types.array.ListDumper)

    def tearDown(self) -> None:
        print('------------------- CLEANING UP -------------------\n')
        queries = ["DELETE FROM test_schema.user"]
        self.conn = psycopg.connect(self.conn_str)
        for q in queries:
            self.conn.execute(q)
        self.conn.close()

    def test_crud(self):
        user = user_db.User(
            first_name='Bryan',
            last_name='Hughes',
            user_state='living',
            user_type='BIG_SHOT',
            email='hughesb@gmail.com',
            user_token=str(uuid.uuid4()),
            enabled=True,
            number_value=5)
        user.created_on.GetCurrentTime()
        user.my_array.extend([100, 101])

        user_1 = user_db.create(self.conn, user)
        print(f'user_1\n-------\n{user_1}')

        self.assertEqual(user.first_name, user_1.first_name)
        self.assertEqual(user.last_name, user_1.last_name)
        self.assertEqual(user.user_type, user_1.user_type)
        self.assertFalse(user.HasField('user_id'))
        self.assertIsNotNone(user_1.user_id)
        self.assertEqual([100, 101], user_1.my_array)
        self.assertEqual(0, user_1.version)

        user_2 = user_db.read(self.conn, user_id=user_1.user_id)
        print(f'user_2\n-------\n{user_2}')

        self.assertEqual(user_1.first_name, user_2.first_name)
        self.assertEqual(user_1.last_name, user_2.last_name)
        self.assertEqual(user_1.user_id, user_2.user_id)
        self.assertEqual([100, 101], user_2.my_array)

        user_2.first_name = 'Big Chief'
        user_2.user_type = user_db.User.LITTLE_SHOT
        user_2.updated_on.GetCurrentTime()
        user_2.my_array.extend([200])

        user_3 = user_db.update(self.conn, user=user_2)
        print(f'user_3\n-------\n{user_3}')

        self.assertEqual(user_3.first_name, user_2.first_name)
        self.assertEqual(user_3.last_name, user_2.last_name)
        self.assertEqual(user_3.user_id, user_2.user_id)
        self.assertEqual([100, 101, 200], user_2.my_array)
        self.assertEqual(user_3.version, 1)

        self.conn.close()


if __name__ == '__main__':
    unittest.main()
