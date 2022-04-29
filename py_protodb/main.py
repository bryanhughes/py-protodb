#!/usr/bin/env python
import getopt
import sys

from config import Config
from database import Database
from proto_gen import ProtoGen
from code_gen import CodeGen


def generate(argv):
    print(f'========================================================================')
    print(f'                              py-protodb')
    print(f'========================================================================\n')
    try:
        opts, args = getopt.getopt(argv, "hc", ["help", "config="])
    except getopt.GetoptError as error:
        print(f'usage: --help | --c <config> : {error}')
        sys.exit(2)

    c = './py-protodb.yaml'
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print(f'usage: --help | --c <config>')
            sys.exit()
        elif opt in ("-c", "--config"):
            c = arg

    print(f'Using config: {c}')
    config = Config(c)

    database = Database(config)
    proto = ProtoGen(config, database)
    proto.generate_protos()

    # Compile the protobuffers
    proto.compile_all()

    codegen = CodeGen(config, database)
    codegen.generate_code()

    print(f'\n================================ DONE ==================================\n')


if __name__ == '__main__':
    generate(sys.argv[1:])

