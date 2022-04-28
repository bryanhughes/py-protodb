#!/usr/bin/env python
import re
from typing import Tuple, List, Any

import sqlparse


class InvalidSQLError(Exception):
    pass


def parse_insert_update_query(sql: str, parsed) -> Tuple[str, List[str], List[str]]:
    in_params = []
    bind_params = []
    holders = []
    for token in parsed[0].tokens:
        results = find_placeholders(token)
        if len(results) > 0:
            holders = holders + results

    for param in holders:
        in_params.append('%(' + param[1:] + ')s')
        bind_params.append(param[1:])
    sql = re.sub(r"\$([^$),\s]*)", '%s', sql)
    return sql, bind_params, in_params


def find_placeholders(token):
    holders = []
    if token.is_group:
        for t in token.tokens:
            results = find_placeholders(t)
            if len(results) > 0:
                holders = holders + results
    else:
        name = token.normalized
        if name[0:1] == '$':
            holders.append(name)
    return holders


def parse_select_query(sql: str, parsed) -> Tuple[str, List[str], List[str]]:
    sql, bind_params = parse_where_clause(parsed, sql)
    in_params = parse_select_clause(parsed)
    return sql, bind_params, in_params


def parse_delete_query(sql: str, parsed) -> tuple[Any, list[Any], list[Any]]:
    sql, bind_params = parse_where_clause(parsed, sql)
    in_params = []
    return sql, bind_params, in_params


def parse_where_clause(parsed, sql):
    bind_params = []
    for token in parsed[0].tokens:
        if token.value.startswith('WHERE'):
            for expression in token.tokens:
                if expression.is_group:
                    for col in expression.tokens:
                        cname = col.normalized
                        if cname[0:1] == '$':
                            bind_params.append(cname[1:])
                            sql = sql.replace(cname, '%s')
    return sql, bind_params


def parse_returning_clause(parsed):
    in_params = []
    for token in parsed[0].tokens:
        if token.value.startswith('RETURNING'):
            for expression in token.tokens:
                if expression.is_group:
                    for col in expression.tokens:
                        cname = col.normalized
                        if cname[0:1] == '$':
                            in_params.append(cname[1:])
    return in_params


def parse_select_clause(parsed):
    in_params = []
    in_select = False
    for token in parsed[0].tokens:
        if token.value.startswith('SELECT'):
            in_select = True
        elif token.value.startswith('WHERE'):
            return in_params

        if in_select is True and token.is_group:
            for col in token.tokens:
                if col.ttype is None:
                    cname = col.normalized
                    in_params.append(cname)
    return in_params


def parse_query(sql: str) -> Tuple[str, List[str], List[str]]:
    parsed = sqlparse.parse(sql)
    if parsed[0].tokens[0].value == 'INSERT':
        return parse_insert_update_query(sql, parsed)
    elif parsed[0].tokens[0].value == 'UPDATE':
        return parse_insert_update_query(sql, parsed)
    elif parsed[0].tokens[0].value == 'SELECT':
        return parse_select_query(sql, parsed)
    elif parsed[0].tokens[0].value == 'DELETE':
        return parse_delete_query(sql, parsed)
    else:
        raise InvalidSQLError(f'Invalid query. Unexpected first token: {parsed.tokens[0].value}, query: {sql}')
