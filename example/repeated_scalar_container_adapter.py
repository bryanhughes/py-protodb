#!/usr/bin/env python
import psycopg


class RepeatedScalar(psycopg.types.array.ListDumper):
    def __int__(self):
        pass