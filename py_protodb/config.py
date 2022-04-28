#!/usr/bin/env python
import yaml


class InvalidConfigError(Exception):
    pass


class Config:
    def __init__(self, filename: str):
        self.filename = filename
        self.config = {}
        with open(filename, "r") as f:
            self.config = yaml.safe_load(f)

    def get_config(self):
        return self.config
