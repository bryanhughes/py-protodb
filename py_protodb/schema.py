#!/usr/bin/env python
"""Reads the schema objects from an existing postgres database
"""
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict

from config import Config

__author__ = "Bryan Hughes"
__copyright__ = "Copyright 2022, Gruvy, Inc."
__credits__ = ["Bryan Hughes"]
__license__ = "Apache"
__version__ = "0.9.0"
__maintainer__ = "Bryan Hughes"
__email__ = "bryan@gruvy.io"
__status__ = "Development"


class InvalidParseError(Exception):
    pass


class IndexType(Enum):
    PRIMARY_KEY = 1
    UNIQUE = 2
    NON_UNIQUE = 3


class RelType(Enum):
    ZERO_ONE_OR_MORE = 1
    MANY_TO_MANY = 2


@dataclass()
class BindVar:
    name: str
    data_type: str


@dataclass()
class CustomQuery:
    name: str
    query: str
    result_set: List[BindVar]


@dataclass()
class Query:
    name: str
    fun_name: str
    fun_args: str
    in_params: str
    bind_params: str
    query: str
    record: str
    default_record: str
    map: str


@dataclass()
class Column:
    table_name: str
    table_schema: str
    name: str
    data_type: str
    udt_name: str
    default: str
    alias: str
    ordinal_position: int = 0
    is_nullable: bool = False
    is_sequence: bool = False
    is_pkey: bool = False
    is_virtual: bool = False
    is_excluded: bool = False
    is_input: bool = False
    is_version: bool = False
    select_xform: str = None
    insert_xform: str = None
    update_xform: str = None
    valid_values: List[str] = None


@dataclass()
class Index:
    table_name: str
    table_schema: str
    name: str
    type: IndexType
    columns: List[str] = None
    is_list: bool = False
    is_lookup: bool = False
    comment: str = None


@dataclass()
class ForeignColumn:
    foreign_name: str
    local_name: str
    ordinal_position: int = 0


@dataclass()
class ForeignRel:
    constraint_name: str
    foreign_schema: str
    foreign_table: str
    foreign_columns: List[ForeignColumn]


@dataclass()
class ProtoMap:
    field_name: str
    column: Column
    relation: ForeignRel


@dataclass()
class Table:
    schema: str
    name: str
    columns: Dict[str, Column]                  # Keyed by column_name
    indexes: Dict[str, Index]                   # Keyed by index_name
    relations: List[ForeignRel]
    mappings: Dict[str, CustomQuery]            # Custom query mappings
    query_dict: Dict[str, Query]                # Query dict for code generation
    sequence: str
    select_list: List[str]
    insert_list: List[str]
    update_list: List[str]
    pkey_list: List[str]
    default_list: List[str]
    last_ordinal: int = 0
    has_valid_values: bool = False
    has_dates: bool = False
    has_timestamps: bool = False
    has_arrays: bool = False
    has_version: bool = False
    version_column: str = None
    proto_extensions: str = None

    @property
    def fqn(self):
        return f'{self.schema}.{self.name}'


@dataclass()
class Schema:
    name: str
    tables: Dict[str, Table] = None                 # Keyed by table_name
    columns: Dict[str, Column] = None               # Keyed by table_name.column_name
    indexes: Dict[str, Index] = None                # Keyed by index_name

    def __init__(self, config: Config):
        self.config = config
        self.conn = self.connect()

    def connect(self):
        pass

    def execute_query(self, query: str, params: tuple):
        pass

    def get_column_datatype(self, oid):
        pass

    def get_table(self, name: str):
        return self.tables[name]

    def get_column(self, table_name: str, name: str):
        return self.columns[f'{table_name}.{name}']

    def get_index(self, name: str):
        return self.indexes[name]
