import unittest

from code_gen import CodeGen
from config import Config
from database import Database


class CodeGenTestCase(unittest.TestCase):
    def setUp(self):
        self.config = Config('py-protodb.yaml')
        self.database = Database(self.config)

    def test_write_protos(self):
        codegen = CodeGen(self.config, self.database)
        codegen.generate_code()


if __name__ == '__main__':
    unittest.main()
