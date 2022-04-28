#!/usr/bin/env python
from typing import List, Tuple

import psycopg

import query_parser
from config import Config, InvalidConfigError
from postgres import Postgres
from schema import Table, CustomQuery, Schema, BindVar, Column, ForeignRel


def ensure_fqn(fqn_or_name):
    if fqn_or_name.find('.') == -1:
        return 'public.' + fqn_or_name
    return fqn_or_name


class Database:
    def __init__(self, config: Config):
        self.config = config
        self.schemas = {}

        dbname = config.get_config()['database']['database']
        uname = config.get_config()['database']['user']
        print(f'Reading database: {dbname} using {uname}')

        schema_names = self.config.get_config()['generator']['schemas']
        if schema_names is []:
            schema_names = ['public']
        for name in schema_names:
            schema = Postgres(name, self.config)
            self.schemas[name] = schema

        # Now process the rest of the configs
        self.maybe_inject_version_column()
        self.process_excluded_cols()
        self.process_extensions()
        self.process_custom_mappings()
        self.process_transforms()

    def process_excluded_cols(self):
        print('\nProcessing excluded columns')
        print('--------------------------------------------------------')
        excluded_cols = self.config.get_config()['generator']['excluded_columns']
        for excluded_col in excluded_cols:
            fqn = excluded_col['table']
            cols = excluded_col['columns']
            if fqn is None or cols is None:
                raise InvalidConfigError(f'Invalid configuration while processing excluded_columns: {excluded_col}')
            parts = fqn.split('.')
            if len(parts) == 1:
                s = 'public'
                tn = parts[0]
            else:
                s = parts[0]
                tn = parts[1]
            table = self.schemas[s].tables[tn]
            print(f'    Excluding columns {cols} on table {s}.{tn}')
            for col in cols:
                c = table.columns[col]
                if c.is_pkey:
                    print(f'    WARNING: Not able to exclude a primary key column {col}.')
                else:
                    c.is_excluded = True
                    table.select_list.remove(col)
                    table.insert_list.remove(col)
                    table.update_list.remove(col)

    def process_extensions(self):
        print('\nProcessing extensions')
        print('--------------------------------------------------------')
        extensions = self.config.get_config()['generator']['extensions']
        for extension in extensions:
            fqn = extension['table']
            ext = extension['extension']
            if fqn is None or ext is None:
                raise InvalidConfigError(f'Invalid configuration while processing extensions: {extensions}')
            parts = fqn.split('.')
            if len(parts) == 1:
                s = 'public'
                tn = parts[0]
            else:
                s = parts[0]
                tn = parts[1]
            print(f'    {s}.{tn} : {ext}')
            table = self.schemas[s].tables[tn]
            if table is None:
                raise InvalidConfigError(f'Invalid configuration while processing extensions. Table {s}.{tn} not found')
            table.proto_extensions = ext

    def process_custom_mappings(self):
        print('\nProcessing custom mappings')
        print('--------------------------------------------------------')
        mappings = self.config.get_config()['generator']['mapping']
        for m in mappings:
            fqn = ensure_fqn(m['table'])
            print(f'Table: {fqn}')
            queries = m['queries']
            if fqn is None or queries is None:
                raise InvalidConfigError(f'Invalid configuration while processing mapping: {m}')
            parts = fqn.split('.')
            s = parts[0]
            tn = parts[1]
            t = self.schemas[s].tables[tn]
            if t is None:
                raise InvalidConfigError(f'Invalid configuration while processing extensions. Table {s}.{tn} not found')

            for query in queries:
                n = query['name']
                q = query['query'].lower()
                if q.find("*") > 1:
                    q = self.expand_sql(t, q)

                print(f'        {n} : {q}')
                rs = []
                if q[0: 6] != 'select' and q.find('returning') == -1:
                    rs = []
                elif q[0: 6] != 'select' and q.find('returning') > -1:
                    # Handle insert / update statement returning a result set
                    rs = self.parse_query(self.schemas[s], self.returning_clause(t.fqn, q))
                elif q[0: 6] == 'select' and q.find('returning') == -1:
                    # The select is the result set
                    rs = self.parse_query(self.schemas[s], self.strip_where_clause(q))
                custom_query = CustomQuery(n, q, rs)
                t.mappings[n] = custom_query

    def expand_sql(self, table: Table, query: str):
        clause = self.build_select_list(table)
        query.replace("*", ', '.join(clause))
        return query

    @staticmethod
    def parse_query(schema: Schema, query: str):
        rs = []
        cur = schema.execute_query(query, ())
        desc = cur.description
        for d in desc:
            oid = d[1]
            dt = schema.get_column_datatype(oid)
            rs.append(BindVar(d[0], dt))
        return rs

    @staticmethod
    def returning_clause(tname: str, query: str):
        pos = query.find('returning')
        if pos != -1:
            clause = query[pos + 9:]
            return 'select ' + clause + ' from ' + tname
        return ''

    @staticmethod
    def strip_where_clause(query: str):
        pos = query.find('where')
        if pos == -1:
            return query
        return query[0: pos]

    def process_transforms(self):
        print('\nProcessing transforms')
        print('--------------------------------------------------------')
        transforms = self.config.get_config()['generator']['transforms']
        for x in transforms:
            fqn = ensure_fqn(x['table'])

            parts = fqn.split('.')
            s = parts[0]
            tn = parts[1]
            t = self.schemas[s].tables[tn]
            if t is None:
                raise InvalidConfigError(f'Invalid configuration while processing extensions. Table {s}.{tn} not found')

            print(f'Table: {fqn}')
            xforms = x['xforms']

            if 'select' in xforms:
                self.process_select_xforms(t, xforms['select'])
            if 'insert' in xforms:
                self.process_insert_xforms(t, xforms['insert'])
            if 'update' in xforms:
                self.process_update_xforms(t, xforms['update'])

    @staticmethod
    def process_select_xforms(table: Table, xform: List):
        for x in xform:
            cname = x['column']
            dtype = x['data_type']
            xform = x['xform']
            if cname in table.columns:
                column = table.columns[cname]
                column.select_xform = xform
            else:
                print(f'        INFO: Adding virtual column {cname} to {table.fqn} from transform {x}')
                virtual_col = Column(table.name, table.schema, cname, 'virtual', dtype, '', '', 99, True, False,
                                     False, True)
                virtual_col.select_xform = xform
                table.columns[cname] = virtual_col
                table.select_list.append(cname)
                table.insert_list.append(cname)
                table.update_list.append(cname)

    @staticmethod
    def process_insert_xforms(table: Table, xform: List):
        for x in xform:
            cname = x['column']
            xform = x['xform']
            if cname in table.columns:
                column = table.columns[cname]
                column.insert_xform = xform
                if column.is_excluded:
                    # Put the column back into the insert_list
                    table.insert_list.append(cname)
            else:
                raise InvalidConfigError(f'Invalid configuration while processing xforms {x}. Column {cname} not '
                                         f'found in table {table.fqn}')

    @staticmethod
    def process_update_xforms(table: Table, xform: List):
        for x in xform:
            cname = x['column']
            xform = x['xform']
            if cname in table.columns:
                column = table.columns[cname]
                column.update_xform = xform
                if column.is_excluded:
                    # Put the column back into the update_list
                    table.update_list.append(cname)
            else:
                raise InvalidConfigError(f'Invalid configuration while processing xforms {x}. Column {cname} not '
                                         f'found in table {table.fqn}')

    def build_queries(self, table: Table):
        queries = {}
        insert_sql = self.build_insert_sql(table)
        sql, bind_params, in_params = query_parser.parse_query(insert_sql)
        queries['INSERT'] = (sql, bind_params, in_params)
        print(f'    {sql}')

        update_sql = self.build_update_sql(table)
        sql, bind_params, in_params = query_parser.parse_query(update_sql)
        print(f'    {sql}')
        queries['UPDATE'] = (sql, bind_params, in_params)

        delete_sql = self.build_delete_sql(table)
        sql, bind_params, in_params = query_parser.parse_query(delete_sql)
        print(f'    {sql}')
        queries['DELETE'] = (sql, bind_params, in_params)

        select_sql = self.build_select_sql(table)
        sql, bind_params, in_params = query_parser.parse_query(select_sql)
        print(f'    {sql}')
        queries['SELECT'] = (sql, bind_params, in_params)

        # Now build the foreign key updates
        for rel in table.relations:
            update_sql = self.build_fkey_sql(table, rel)
            sql, bind_params, in_params = query_parser.parse_query(update_sql)
            print(f'    {sql}')
            queries[f'{rel.constraint_name.upper()}_UPDATE'] = (sql, bind_params, in_params)

        return queries

    def build_fkey_sql(self, table: Table, rel: ForeignRel):
        where_clause = []
        for cname in table.pkey_list:
            where_clause.append(cname + ' = $' + cname)

        if table.version_column is not None:
            where_clause.append(table.version_column + ' = ' + table.version_column + ' + 1')

        clause = self.build_fkey_update_clause(table, rel)
        returning_clause = self.build_select_list(table)
        return "UPDATE " + table.schema + "." + table.name + " SET " + ', '.join(clause) + " WHERE " + \
               ' AND '.join(where_clause) + " RETURNING " + ', '.join(returning_clause)


    def build_insert_sql(self, table: Table) -> str:
        (clause, params) = self.build_insert_tuple(table)
        returning_clause = self.build_select_list(table)
        return "INSERT INTO " + table.schema + "." + table.name + " (" + ', '.join(clause) + ") VALUES (" + \
               ', '.join(params) + ") RETURNING " + ', '.join(returning_clause)

    @staticmethod
    def build_insert_tuple(table) -> Tuple:
        clause = []
        param = []
        for cname in table.insert_list:
            col = table.columns[cname]
            if col.is_sequence is True:
                continue
            elif col.is_virtual is True:
                continue
            elif col.is_version is True:
                p = '0'
            elif col.insert_xform is not None:
                p = col.insert_xform
            else:
                p = '$' + col.name
            clause.append(col.name)
            param.append(p)
        return clause, param

    @staticmethod
    def build_select_list(table: Table) -> List[str]:
        clause = []
        for cname in table.select_list:
            col = table.columns[cname]
            if col.select_xform is not None:
                clause.append(col.select_xform + ' AS ' + col.name)
            elif col.is_virtual is True:
                continue
            else:
                clause.append(col.name)
        return clause

    @staticmethod
    def build_returning_list(table: Table) -> List[str]:
        clause = []
        for cname in table.select_list:
            col = table.columns[cname]
            if col.select_xform is not None:
                clause.append(col.name)
            elif col.is_virtual is True:
                continue
            else:
                clause.append(col.name)
        return clause

    def build_update_sql(self, table: Table):
        where_clause = []
        for cname in table.pkey_list:
            where_clause.append(cname + ' = $' + cname)

        if table.version_column is not None:
            where_clause.append(table.version_column + ' = ' + table.version_column + ' + 1')

        clause = self.build_update_clause(table)
        returning_clause = self.build_select_list(table)
        return "UPDATE " + table.schema + "." + table.name + " SET " + ', '.join(clause) + " WHERE " + \
               ' AND '.join(where_clause) + " RETURNING " + ', '.join(returning_clause)

    def build_select_sql(self, table: Table):
        where_clause = []
        for cname in table.pkey_list:
            where_clause.append(cname + ' = $' + cname)

        select_clause = self.build_select_list(table)
        return "SELECT " + ", ".join(select_clause) + " FROM " + table.schema + "." + table.name + " WHERE " + \
               ' AND '.join(where_clause)

    @staticmethod
    def build_fkey_update_clause(table: Table, rel: ForeignRel) -> List[str]:
        clause = []
        for fcol in rel.foreign_columns:
            cname = fcol.local_name
            col = table.columns[cname]
            if col.is_sequence is True:
                continue
            elif col.is_virtual is True:
                continue
            elif col.is_version is True:
                clause.append(cname + ' = ' + cname + ' +1')
            elif col.update_xform is not None:
                clause.append(cname + ' = ' + col.update_xform)
            else:
                clause.append(cname + ' = $' + col.name)
        return clause

    @staticmethod
    def build_update_clause(table) -> List[str]:
        clause = []
        for cname in table.update_list:
            col = table.columns[cname]
            if col.is_sequence is True:
                continue
            elif col.is_virtual is True:
                continue
            elif col.is_version is True:
                clause.append(cname + ' = ' + cname + ' +1')
            elif col.update_xform is not None:
                clause.append(cname + ' = ' + col.update_xform)
            else:
                clause.append(cname + ' = $' + col.name)
        return clause

    @staticmethod
    def build_delete_sql(table: Table):
        where_clause = []
        for cname in table.pkey_list:
            where_clause.append(cname + ' = $' + cname)
        return "DELETE FROM " + table.schema + "." + table.name + " WHERE " + " AND ".join(where_clause)

    def maybe_inject_version_column(self):
        inject = self.config.get_config()['generator']['inject_version_column']
        if inject:
            version_cname = self.config.get_config()['generator']['version_column']
            print('Maybe Inject Version Column')
            print('--------------------------------------------------------')
            for schema in self.schemas.values():
                conn = schema.connect()
                for table in schema.tables.values():
                    print(f'     {table.schema}.{table.name}.{version_cname}...', end='')
                    if table.has_version:
                        print('exists')
                        continue

                    print('does not exist, ADDING')
                    alter_sql = f'ALTER TABLE {table.schema}.{table.name} ADD COLUMN {version_cname} bigint'
                    try:
                        conn.execute(alter_sql)
                        conn.commit()
                        table.version_column = version_cname
                    except (Exception, psycopg.DatabaseError) as error:
                        print(f'FAILED. {error.pgcode[:2]} - {error.pgcode}')
                conn.close()
