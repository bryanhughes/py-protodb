import unittest

from config import Config
from database import Database
from proto_gen import ProtoGen


class ProtoGenTestCase(unittest.TestCase):
    def setUp(self):
        self.config = Config('../py-protodb.yaml')
        self.database = Database(self.config)

    def test_write_protos(self):
        proto = ProtoGen(self.config, self.database)
        proto.generate_protos()


if __name__ == '__main__':
    unittest.main()
