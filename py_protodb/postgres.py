#!/usr/bin/env python
from collections import OrderedDict

import psycopg
import postgres_datatypes

from config import Config
from schema import Schema, Table, Column, Index, IndexType, ForeignRel, ForeignColumn, InvalidParseError

# Postgres statements

READ_TABLES = """SELECT
        t.table_schema,
        t.table_name
    FROM
        pg_namespace ns
        JOIN pg_class cls ON
            cls.relnamespace = ns.oid
        JOIN information_schema.tables t ON
            t.table_name = cls.relname
            AND t.table_schema = ns.nspname
    WHERE
        t.table_type = 'BASE TABLE'
        AND t.table_name NOT IN (_EXCLUDED_)
        AND ns.nspname = %s
        AND cls.relispartition = 'f'
    ORDER BY table_name"""

READ_COLUMNS = """SELECT
        c.column_name,
        c.ordinal_position,
        c.data_type,
        c.udt_name::regtype::text,
        c.column_default,
        c.is_nullable,
        CASE WHEN pa.attname is null THEN false ELSE true END is_pkey,
        CASE WHEN pg_get_serial_sequence(table_schema || '.' || table_name, column_name) is null 
             THEN false ELSE true END is_seq
    FROM
        pg_namespace ns
    JOIN pg_class t ON
        t.relnamespace = ns.oid
        AND t.relkind in ('r', 'p')
        AND t.relname = %s
    JOIN information_schema.columns c ON
        c.table_schema = ns.nspname
        AND c.table_name = t.relname
    LEFT OUTER JOIN pg_index pi ON
        pi.indrelid = t.oid AND pi.indisprimary = true
    LEFT OUTER JOIN pg_attribute pa ON
        pa.attrelid = pi.indrelid
        AND pa.attnum = ANY(pi.indkey)
        AND pa.attname = c.column_name
    WHERE
        ns.nspname = %s
    ORDER BY table_schema, table_name, ordinal_position"""

READ_INDEXES = """SELECT
        i.relname AS index_name,
        a.attname AS column_name,
        ix.indisunique is_unique,
        ix.indisprimary is_pkey,
        obj_description(i.oid) AS comment
    FROM
        pg_class t,
        pg_class i,
        pg_index ix,
        pg_attribute a,
        pg_namespace ns
    WHERE
        t.oid = ix.indrelid
        AND i.oid = ix.indexrelid
        AND a.attrelid = t.oid
        AND a.attnum = ANY(ix.indkey)
        AND t.relkind = 'r'
        AND t.relname = %s
        AND t.relnamespace = ns.oid
        AND ns.nspname = %s
    ORDER BY
        index_name, column_name"""

READ_FOREIGN_RELATIONSHIPS = """SELECT DISTINCT
        rc.constraint_name,
        f_kcu.table_schema AS foreign_schema,
        f_kcu.table_name AS foreign_table,
        f_kcu.column_name AS foreign_column,
        kcu.column_name AS local_column,
        f_kcu.ordinal_position
    FROM
        information_schema.key_column_usage kcu
        JOIN information_schema.referential_constraints rc ON
            rc.constraint_schema = kcu.constraint_schema
            AND rc.constraint_name = kcu.constraint_name
        JOIN information_schema.key_column_usage f_kcu ON
            f_kcu.constraint_schema = rc.unique_constraint_schema
            AND f_kcu.constraint_name = rc.unique_constraint_name
            AND f_kcu.ordinal_position = kcu.position_in_unique_constraint
    WHERE
        kcu.table_schema = %s
        AND kcu.table_name = %s
        AND kcu.position_in_unique_constraint IS NOT NULL
    ORDER BY
        foreign_schema, foreign_table"""

READ_CHECK_CONSTRAINTS = """SELECT
        pg_get_constraintdef(pgc.oid) as constr
    FROM pg_constraint pgc
        JOIN pg_namespace nsp ON nsp.oid = pgc.connamespace
        JOIN pg_class cls ON pgc.conrelid = cls.oid
        LEFT JOIN information_schema.constraint_column_usage ccu ON
            pgc.conname = ccu.constraint_name AND
            nsp.nspname = ccu.constraint_schema
    WHERE
        contype ='c'
        AND ccu.table_schema = %s
        AND ccu.table_name = %s"""


def parse_name(param):
    pos = param.find("'", 1)
    return param[1: pos]


class Postgres(Schema):
    def __init__(self, schema_name: str, cfg: Config):
        self.config = cfg
        self.name = schema_name
        Schema.__init__(self, cfg)
        self.read_tables()

    def connect(self):
        host = self.config.get_config()['database']['host']
        port = self.config.get_config()['database']['port']
        dbname = self.config.get_config()['database']['database']
        user = self.config.get_config()['database']['user']
        password = self.config.get_config()['database']['password']
        conn_str = f'host={host} port={port} dbname={dbname} user={user} password={password}'
        return psycopg.connect(conn_str)

    def execute_query(self, query: str, params: tuple):
        return self.conn.execute(query, params)

    def get_column_datatype(self, oid: int):
        query = """
            SELECT 
                typname
            FROM
                pg_type
            WHERE
                oid = %(oid)s"""
        cur = self.conn.execute(query, {'oid': oid})
        (t, ) = cur.fetchone()
        cur.close()
        return t

    def read_tables(self):
        print(f'Reading tables from schema: {self.name}')
        print(f'--------------------------------------------------------')
        excluded_tables = ",".join([f'\'{s}\'' for s in self.config.get_config()['generator']['excluded_tables']])
        print(f'Excluding tables: {excluded_tables}')
        query = READ_TABLES.replace('_EXCLUDED_', excluded_tables)
        cur = self.conn.execute(query, (self.name,))
        for record in cur:
            (s, n) = record
            table = Table(s, n, OrderedDict(), {}, [], {}, {}, '', [], [], [], [], [])
            if self.tables is None:
                self.tables = {}
            self.tables[n] = table
        cur.close()
        for table in self.tables:
            t = self.tables[table]
            print(f'    {t.schema}.{t.name}')
            self.read_columns(t)
            self.read_indexes(t)
            self.read_relationships(t)
            self.read_constraints(t)
            print(f'            SELECT: {t.select_list}')
            print(f'            INSERT: {t.insert_list}')
            print(f'            UPDATE: {t.update_list}')
            print(f'    -------------------------------------------------')
        print('')

    def read_columns(self, table: Table):
        version = self.config.get_config()['generator']['version_column']
        cur = self.conn.execute(READ_COLUMNS, (table.name, table.schema))
        for record in cur:
            (cname, opos, dtype, udt_name, coldef, is_nullable, is_pkey, is_seq) = record
            print(f'            Column: {cname}, {opos}, {dtype}, {udt_name}, {is_nullable}, {is_pkey}, {is_seq}')
            column = Column(table.name, table.schema, cname, dtype, udt_name, coldef, '', opos,
                            (is_nullable == 'YES'), is_seq, is_pkey)

            table.columns[cname] = column
            if self.columns is None:
                self.columns = OrderedDict()
            self.columns[f'{table.name}.{cname}'] = column

            if cname == version:
                column.is_version = True
                table.has_version = True
                table.version_column = cname

            if is_seq:
                table.sequence = cname
            if is_pkey:
                table.pkey_list.append(cname)

            if table.has_timestamps or postgres_datatypes.is_timestamp(udt_name):
                table.has_timestamps = True

            if table.has_arrays or postgres_datatypes.is_array(dtype):
                table.has_arrays = True

            if table.has_dates or postgres_datatypes.is_date(udt_name):
                table.has_dates = True

            if column.is_version is not True:
                if column.is_sequence is not True:
                    if coldef:
                        if table.default_list is None:
                            table.default_list = []
                        table.default_list.append(cname)

            table.version_column = version
            table.select_list.append(cname)
            if is_pkey is False and is_seq is False:
                table.update_list.append(cname)
                table.insert_list.append(cname)
        cur.close()

    def read_indexes(self, table: Table):
        if self.indexes is None:
            self.indexes = {}

        if table.indexes is None:
            table.indexes = {}

        cur = self.conn.execute(READ_INDEXES, (table.name, table.schema))
        curname = None
        name_list = []
        index = None
        for record in cur:
            (iname, cname, is_unique, is_pkey, comment) = record
            if iname != curname:
                # Store the current index and start a new one
                if index is not None:
                    index.columns = name_list
                    print(f'             Index: {index.name}, {index.type}, {name_list}')
                    self.indexes[index.name] = index
                    table.indexes[index.name] = index

                idx_type = IndexType.NON_UNIQUE
                if is_pkey:
                    idx_type = IndexType.PRIMARY_KEY
                elif is_unique:
                    idx_type = IndexType.UNIQUE

                is_list = False
                is_lookup = False
                if iname.startswith('list_'):
                    is_list = True
                elif iname.startswith('lookup_'):
                    is_lookup = True

                index = Index(table.name, table.schema, iname, idx_type, [], is_list, is_lookup, comment)

            # Add cname to list
            name_list.append(cname)
            curname = iname

        if index is not None:
            index.columns = name_list
            print(f'             Index: {index.name}, {index.type}, {name_list}')
            self.indexes[index.name] = index
            table.indexes[index.name] = index
        cur.close()

    def read_relationships(self, table: Table):
        if table.relations is None:
            table.relations = {}

        cur = self.conn.execute(READ_FOREIGN_RELATIONSHIPS, (table.schema, table.name))
        curname = None
        fcol_list = []
        frel = None
        for record in cur:
            (constr_name, fschema, ftable, fcname, lcname, opos) = record
            fcol = ForeignColumn(fcname, lcname, opos)
            if constr_name != curname:
                # Store the current foreign_relationship and start a new one
                if frel is not None:
                    frel.foreign_columns = fcol_list
                    print(f'          Relation: {frel.constraint_name}, {frel.foreign_schema}, {frel.foreign_table}, '
                          f'{frel.foreign_columns}')
                    table.relations.append(frel)

                frel = ForeignRel(constr_name, fschema, ftable, [])
                fcol_list = []

            fcol_list.append(fcol)
            curname = constr_name

        if frel is not None:
            frel.foreign_columns = fcol_list
            print(f'          Relation: {frel.constraint_name}, {frel.foreign_schema}, {frel.foreign_table}, '
                  f'{frel.foreign_columns}')
            table.relations.append(frel)
        cur.close()
        self.exclude_from_update(table)

    @staticmethod
    def exclude_from_update(table: Table):
        for rel in table.relations:
            for fcol in rel.foreign_columns:
                cname = fcol.local_name
                try:
                    table.update_list.remove(cname)
                except ValueError as _e:
                    pass

    def read_constraints(self, table: Table):
        cur = self.conn.execute(READ_CHECK_CONSTRAINTS, (table.schema, table.name))
        for record in cur:
            (constr,) = record
            # We need to parse the constraint:
            #   CHECK ((number_value > 1))
            #   CHECK (((user_type)::text = ANY ((ARRAY['BIG SHOT'::character varying, 'LITTLE-SHOT'::character varying,
            #                                           'BUSY_GUY'::character varying, 'BUSYGAL'::character varying,
            #                                           '123FUN'::character varying])::text[])))
            if constr.startswith("CHECK ((("):
                s = 9
                e = constr.find(")")
                cname = constr[s: e]
                s = constr.find("[") + 1
                e = constr.find("]")
                col = table.columns[cname]
                if col is None:
                    cur.close()
                    raise InvalidParseError(f'Failed to lookup column {cname} from table {table.schema}.{table.name}')
                col.valid_values = [parse_name(x.strip()) for x in constr[s: e].split(',')]
                print(f'      Valid Values: {col.valid_values}')
                table.has_valid_values = True
        cur.close()
