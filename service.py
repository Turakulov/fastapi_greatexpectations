import os

import great_expectations as ge
from great_expectations.datasource.fluent.sql_datasource import (
    SQLDatasource,
    TableAsset,
)
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)

from config import settings
from models.expectation import Event

SCRIPT_DIR = os.path.dirname(__file__)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class GxSession(object):
    # keep state of the great_expectations session
    __metaclass__ = Singleton

    context: AbstractDataContext
    data_connector_name: str
    datasource_name: str
    expectation_suite_name: str
    checkpoint_name: str

    def __init__(self):
        self.context = ge.get_context()
        self.data_connector_name = None
        self.table_name = None
        self.schema_name = None
        self.datasource_name = None
        self.expectation_suite_name = None
        self.checkpoint_name = None

    @property
    def gx_session_config(self):
        return (self.data_connector_name,
                self.datasource_name,
                self.expectation_suite_name,
                self.checkpoint_name)

    @gx_session_config.setter
    def gx_session_config(self, gx_mapping: Event):
        self.data_connector_name = f"connector_{gx_mapping.connection}"
        self.table_name = gx_mapping.table_name
        self.schema_name = gx_mapping.schema_name
        self.datasource_name = f"{gx_mapping.source}.{gx_mapping.schema_name}.{gx_mapping.table_name}"
        self.expectation_suite_name = f"{gx_mapping.source}.{gx_mapping.schema_name}.{gx_mapping.table_name}.suite"
        self.checkpoint_name = f"{gx_mapping.source}.{gx_mapping.schema_name}.{gx_mapping.table_name}.checkpoint"

    def get_datasource_config(self) -> str:
        with open(f'{SCRIPT_DIR}/templates/datasource.yaml', 'r', encoding='utf-8') as file:
            config = file.read()
            config = config.replace('\"{{name}}\"', self.datasource_name) \
                .replace('\"{{connection_string}}\"', settings.sql_url.__str__()) \
                .replace('\"{{data_connector_name}}\"', self.data_connector_name) \
                .replace('\"{{table_name}}\"', self.table_name) \
                .replace('\"{{schema_name}}\"', self.schema_name)
        return config

    def get_checkpoint_config(self) -> str:
        with open(f'{SCRIPT_DIR}/templates/checkpoint.yaml', 'r', encoding='utf-8') as file:
            config = file.read()
            config = config.replace('\"{{name}}\"', self.checkpoint_name) \
                .replace('\"{{run_name_template}}\"', f'\"%Y-%m-%d %H:%M:%S|{self.datasource_name}\"') \
                .replace('\"{{datasource_name}}\"', self.datasource_name) \
                .replace('\"{{data_connector_name}}\"', self.data_connector_name) \
                .replace('\"{{data_asset_name}}\"', self.table_name) \
                .replace('\"{{expectation_suite_name}}\"', self.expectation_suite_name)
        return config
