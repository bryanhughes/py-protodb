#!/usr/bin/env python
"""Maps postgres datatype to Python
"""


def is_int64(dt: str):
    match dt:
        case 'bigint':
            return True
        case 'bigserial':
            return True
        case 'serial8':
            return True
        case _:
            return False


def is_timestamp(udt_name):
    if udt_name.startswith('timestamp'):
        return True
    return False


def is_array(dtype):
    if dtype == 'ARRAY':
        return True
    return False


def is_date(udt_name):
    if udt_name == 'date':
        return True
    return False


def sql_to_proto_datatype(dt: str):
    # special cases
    if dt.startswith('bit'):
        return 'int32'

    if dt.startswith('char'):
        return 'string'

    if dt.startswith('numeric') or dt.startswith('decimal'):
        return 'double'

    if dt.startswith('timestamp'):
        return 'google.protobuf.Timestamp'

    match dt:
        case 'bigint' | 'bigint[]' | '{array,int8}' | 'int8' | 'bigserial' | 'serial8' | 'time':
            return 'int64'
        case 'integer' | 'integer[]' | '{array,int4}' | 'int' | 'int4' | 'smallint' | 'smallint[]' | '{array,int2}' | \
             'int2' | 'smallserial' | 'serial':
            return 'int32'
        case 'boolean' | 'boolean[]' | '{array,boolean}' | 'bool':
            return 'bool'
        case 'bytea':
            return 'bytes'
        case 'varchar' | 'character varying' | 'character varying[]' | 'text' | 'text[]' | '{array,text}' | 'uuid' | \
             'xml':
            return 'string'
        case 'date':
            return 'google.protobuf.Timestamp'
        case 'double precision' | 'float8' | 'money':
            return 'double'
        case 'real' | 'float4':
            return 'float'
        case 'json[]' | 'json':
            return 'string'
        case 'jsonb':
            return 'bytes'
        case _:
            print(f'    [warning] Failed to map postgres datatype to protobuf: {dt}. Using \"bytes\"')
            return 'bytes'
