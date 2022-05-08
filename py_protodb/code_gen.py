#!/usr/bin/env python
import os
from datetime import datetime
from re import sub

from config import Config
from database import Database
from schema import Table


def camel_case(s):
    s = sub(r"([_\-])+", " ", s).title().replace(" ", "")
    return ''.join([s[0].lower(), s[1:]])


def snake_case(s):
    return '_'.join(sub('([A-Z][a-z]+)', r' \1', sub('([A-Z]+)', r' \1', s.replace('-', ' ').
                                                     replace('_', ' '))).split()).lower()


def cap_camel_case(s):
    s = sub(r"([_\-])+", " ", s).title().replace(" ", "")
    return ''.join([s[0].upper(), s[1:]])


class CodeGen:
    def __init__(self, config: Config, database: Database):
        self.config = config
        self.database = database
        self.path = self.config.get_config()['output']['path']
        self.suffix = self.config.get_config()['output']['suffix']
        self.package = self.config.get_config()['output']['package']
        self.index_lookups = self.config.get_config()['generator']['indexed_lookups']
        self.support_record_version = self.config.get_config()['generator']['support_record_version']
        self.inject_version_column = self.config.get_config()['generator']['inject_version_column']
        self.version_column = self.config.get_config()['generator']['version_column']

    def generate_code(self):
        print(f'\n------- Generating Code --------\n')
        for schema in self.database.schemas.values():
            for table in schema.tables.values():
                self.generate(table)

    def generate(self, table: Table):
        code_path = os.path.sep.join([self.path, table.schema])
        suffix = self.config.get_config()['output']['suffix']
        code_fname = os.path.sep.join([code_path, table.name + suffix + '.py'])
        print(f'{code_fname}')
        if not os.path.exists(code_path):
            os.makedirs(code_path)
        self.write_code(code_fname, table)

    def write_code(self, code_fname: str, table: Table):
        with open(code_fname, 'w') as pfile:
            now = datetime.now()
            header = ("#!/usr/bin/env python\n"
                      "# -*- coding: utf-8 -*-\n"
                      "# ------------------------------------------------------------------------------\n"
                      "# This file is automatically generated from the database schema using py-protodb\n"
                      "# database:     " + self.config.get_config()['database']['database'] + "\n"
                      "# user:         " + self.config.get_config()['database']['user'] + "\n"
                      "# generated on: " + now.strftime("%m/%d/%Y, %H:%M:%S") + "\n"
                      "# ----------------- DO NOT MAKE CHANGES DIRECTLY TO THIS FILE! -----------------\n"
                      )
            pfile.write(header)

            cc_name = cap_camel_case(table.name)
            pfile.write(f'from {table.schema} import {table.name}_pb2\n\n')
            pfile.write(f'{cc_name} = {table.name}_pb2.{cc_name}\n\n')

            queries = self.database.build_queries(table)
            self.write_queries(pfile, queries)

            self.maybe_write_enum_value(pfile, table)
            self.maybe_write_value_enum(pfile, table)
            self.write_is_null(pfile, table)
            self.write_not_nullable(pfile, table)
            self.write_to_datetime(pfile)

            self.write_set(pfile, table)
            self.write_proto_in_funs(pfile, table, queries, 'INSERT', 'create')
            self.write_params_in_funs(pfile, table, queries, 'SELECT', 'read')
            self.write_proto_in_funs(pfile, table, queries, 'UPDATE', 'update')
            self.write_proto_in_funs(pfile, table, queries, 'DELETE', 'delete')

            for rel in table.relations:
                qname = rel.constraint_name.upper() + '_UPDATE'
                fname = rel.constraint_name + '_update'
                self.write_proto_in_funs(pfile, table, queries, qname, fname)

            pfile.close()

    @staticmethod
    def write_queries(pfile, queries):
        for query_type in queries:
            (sql, _, _) = queries[query_type]
            pfile.write(f'{query_type} = "{sql}"\n')
        pfile.write('\n\n')

    def write_proto_in_funs(self, pfile, table, queries, query_type, fname):
        query = queries[query_type]
        (_sql, bind_params, in_params) = query
        cc = cap_camel_case(table.name)
        pfile.write(f'def {fname}(conn, {table.name}: {table.name}_pb2.{cc}):\n')
        returning = self.database.build_returning_list(table)
        bind_columns = []
        for bind_cname in bind_params:
            col = table.columns[bind_cname]
            if col.is_nullable is True:
                binding = 'is_null(' + table.name + ', \'' + bind_cname + '\', ' + table.name + '.' + bind_cname + ')'
            elif col.data_type != 'ARRAY':
                binding = 'not_null(' + table.name + ', \'' + bind_cname + '\', ' + table.name + '.' + bind_cname + ')'
            else:
                binding = table.name + '.' + bind_cname

            if col.valid_values is not None:
                binding = col.name + '_value(' + binding + ')'

            if col.udt_name.startswith('timestamp'):
                binding = 'to_datetime(' + binding + ')'

            bind_columns.append(binding)

        params = ', '.join(bind_columns)
        values = ', '.join(returning)
        pfile.write(f'    bind_args = [{params}]\n')
        if query_type == 'DELETE':
            pfile.write(f'    conn.execute({query_type}, bind_args)\n\n\n')
        else:
            pfile.write(f'    cur = conn.execute({query_type}, bind_args)\n')
            pfile.write(f'    result = cur.fetchone()\n')
            pfile.write(f'    if result is None:\n')
            pfile.write(f'        return None\n')
            pfile.write(f'    else:\n')
            pfile.write(f'        ({values}) = result\n')
            pfile.write(f'        return set_fields({values})\n\n\n')

    def write_set(self, pfile, table):
        returning = self.database.build_returning_list(table)
        values = ', '.join(returning)
        pfile.write(f'def set_fields({values}):\n')
        pfile.write(f'    out = User()\n')
        for cname in returning:
            col = table.columns[cname]
            assign = f'out.{cname} = {cname}'
            if col.udt_name == 'uuid':
                assign = f'out.{cname} = str({cname})'
            elif col.valid_values is not None:
                assign = f'out.{cname} = {cname}_enum({cname})'
            elif col.udt_name.startswith('timestamp'):
                assign = f'out.{cname}.FromDatetime({cname})'

            if col.is_nullable:
                pfile.write(f'    if {cname} is not None:\n        {assign}\n')
            elif col.data_type == 'ARRAY':
                pfile.write(f'    if len({cname}) > 0:\n')
                pfile.write(f'        out.{cname}.extend({cname})\n')
            else:
                pfile.write(f'    {assign}\n')
        pfile.write(f'    return out\n\n\n')

    def write_params_in_funs(self, pfile, table, queries, query_type, fname):
        query = queries[query_type]
        (_sql, bind_params, in_params) = query
        params = ', '.join(bind_params)
        pfile.write(f'def {fname}(conn, {params}):\n')
        returning = self.database.build_returning_list(table)
        values = ', '.join(returning)
        pfile.write(f'    cur = conn.execute({query_type}, [{params},])\n')
        pfile.write(f'    result = cur.fetchone()\n')
        pfile.write(f'    if result is None:\n')
        pfile.write(f'        return None\n')
        pfile.write(f'    else:\n')
        pfile.write(f'        ({values}) = result\n')
        pfile.write(f'        return set_fields({values})\n\n\n')

    @staticmethod
    def maybe_write_enum_value(pfile, table):
        cc = cap_camel_case(table.name)
        for cname in table.columns:
            col = table.columns[cname]
            if col.valid_values is not None:
                pfile.write(f'def {cname}_value(enum):\n')
                pfile.write(f'    match enum:\n')
                pfile.write(f'        case None:\n')
                pfile.write(f'            return None\n')
                for val in col.valid_values:
                    val_cc = val.replace('-', '_').replace(' ', '_')
                    if val_cc[0].isdigit():
                        val_cc = '_' + val_cc
                    pfile.write(f'        case {table.name}_pb2.{cc}.{val_cc}:\n')
                    pfile.write(f'            return \'{val}\'\n')
                pfile.write('\n\n')

    @staticmethod
    def maybe_write_value_enum(pfile, table):
        cc = cap_camel_case(table.name)
        for cname in table.columns:
            col = table.columns[cname]
            if col.valid_values is not None:
                pfile.write(f'def {cname}_enum(value):\n')
                pfile.write(f'    match value:\n')
                pfile.write(f'        case None:\n')
                pfile.write(f'            return None\n')
                for val in col.valid_values:
                    val_cc = val.replace('-', '_').replace(' ', '_')
                    if val_cc[0].isdigit():
                        val_cc = '_' + val_cc
                    pfile.write(f'        case \'{val}\':\n')
                    pfile.write(f'            return {table.name}_pb2.{cc}.{val_cc}\n')
                pfile.write('\n\n')

    @staticmethod
    def write_is_null(pfile, table):
        cc = cap_camel_case(table.name)
        pfile.write(f'def is_null({table.name}: {table.name}_pb2.{cc}, field_name, field):\n')
        pfile.write(f'    if {table.name}.HasField(field_name) is True:\n')
        pfile.write(f'        return field\n')
        pfile.write(f'    else:\n')
        pfile.write(f'        return None\n\n\n')

    @staticmethod
    def write_not_nullable(pfile, table):
        cc = cap_camel_case(table.name)
        pfile.write(f'def not_null({table.name}: {table.name}_pb2.{cc}, field_name, field):\n')
        pfile.write(f'    if {table.name}.HasField(field_name) is False:\n')
        pfile.write(f'        raise Exception(\'Field {table.name}: {table.name}_pb2.{cc}.\' + field_name + '
                    f'\' can not be null\')\n')
        pfile.write(f'    return field\n\n\n')

    @staticmethod
    def write_to_datetime(pfile):
        pfile.write('def to_datetime(param):\n')
        pfile.write('    if param is not None:\n')
        pfile.write('        return param.ToDatetime()\n')
        pfile.write('    else:\n')
        pfile.write('        return param\n\n\n')
