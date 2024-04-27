from datetime import datetime
from pytz import timezone

from fastapi import APIRouter, Request, Body
from fastapi.responses import PlainTextResponse
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from config import settings
from models.expectation import Event
from utils.logging import AppLogger

logger = AppLogger.__call__().get_logger()

router = APIRouter(prefix="/expectation", tags=["Expectations"])


@router.get("/list_available_expectation_types/{table}")
def list_available_expectation_types(table: str, request: Request):
    _gx = request.app.state.gx

    _gx.set_asset(table_name=table)

    _validator = _gx.context.get_validator(
        datasource_name=settings.sql_datasource_name,
        data_asset_name=_gx.sql_table_asset.name,
    )

    try:
        return eval(f"_validator.list_available_expectation_types()")
    except InvalidExpectationConfigurationError as e:
        return e.__dict__


@router.post("/run_expectation")
def run_expectation(
        request: Request,
        gx_mapping: Event = Body(
            None,
            description="pass parameters as json dict and in next step unpack to **mapping",
        ),
):
    _gx = request.app.state.gx
    _gx.gx_session_config = gx_mapping
    # print(_gx.datasource_name)
    yaml = YAMLHandler()
    # print(_gx.get_datasource_config())
    _gx.context.add_or_update_datasource(**yaml.load(_gx.get_datasource_config()))
    multi_batch: BatchRequest = BatchRequest(
        datasource_name=_gx.datasource_name,
        data_connector_name=_gx.data_connector_name,
        data_asset_name=_gx.table_name,
    )
    expectation_suite = _gx.context.add_or_update_expectation_suite(
        expectation_suite_name=_gx.expectation_suite_name
    )
    validator = _gx.context.get_validator(
        batch_request=multi_batch,
        expectation_suite_name=_gx.expectation_suite_name
    )

    table_columns = validator.columns()
    print(table_columns)
    for expectation_key, expectations_kwargs in gx_mapping.expectations.items():
        print(expectation_key, expectations_kwargs)
        col_arg = expectations_kwargs.get('column', None)
        min_value = expectations_kwargs.get('min_value', None)
        max_value = expectations_kwargs.get('max_value', None)

        if isinstance(col_arg, str) and col_arg == 'all':
            col_arg = table_columns
        elif isinstance(col_arg, list):
            column_check = all(item in table_columns for item in col_arg)
            if not column_check:
                raise ValueError("Some columns listed in DQ config doesn't exist in table")

        elif isinstance(col_arg, dict):
            raise TypeError("Dictionaries in DQ config are not supported!")

        for col in col_arg:
            _kwargs = {}
            if min_value:
                _kwargs['min_value'] = min_value
            if max_value:
                _kwargs['max_value'] = max_value
            _kwargs["column"] = col

            expectation_config = ExpectationConfiguration(expectation_type=expectation_key, kwargs=_kwargs)
            expectation_suite.add_expectation(expectation_config)

            # print(self._expectation_suite.show_expectations_by_expectation_type())
    _gx.context.save_expectation_suite(expectation_suite)
    _gx.context.add_or_update_checkpoint(**yaml.load(_gx.get_checkpoint_config()))


    results = _gx.context.run_checkpoint(checkpoint_name=_gx.checkpoint_name,
                                         run_time=datetime.now(tz=timezone('Europe/Moscow')),
                                         # result_format="COMPLETE"
                                         )
    _gx.context.build_data_docs()

    return {}
    #
    # _gx.set_asset(table_name=table)
    #
    # _validator = _gx.context.get_validator(
    #     datasource_name=settings.sql_datasource_name,
    #     data_asset_name=_gx.sql_table_asset.name,
    # )
    #
    # try:
    #     return eval(f"_validator.{expectation_type}(**gx_mapping)")
    # except InvalidExpectationConfigurationError as e:
    #     return e.__dict__
    # except Exception as e:
    #     logger.error(f"Error: {e}")
    #     return e.__dict__


@router.get(
    "/doc_expectation/{table}/{expectation_type}", response_class=PlainTextResponse
)
def doc_expectation(
        table: str,
        expectation_type: str,
        request: Request,
):
    _gx = request.app.state.gx

    _gx.set_asset(table_name=table)

    _validator = _gx.context.get_validator(
        datasource_name=settings.sql_datasource_name,
        data_asset_name=_gx.sql_table_asset.name,
    )

    try:
        _doc = eval(f"_validator.{expectation_type}.__doc__")
        return _doc
    except Exception as e:
        logger.error(f"Error: {e}")
        return e.__dict__

# TODO: endpoint to create expectation.py suite >
#  should keep expectation.py suite in memory and update it with each call
#  unless reset or implicit reset after save to database
