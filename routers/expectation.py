from datetime import datetime
from typing import List
import copy

import pandas as pd
from pytz import timezone

from fastapi import APIRouter, Request, Body, UploadFile, File, Depends

from fastapi.responses import PlainTextResponse
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from config import SCRIPT_DIR, settings
from notifications.messages import make_message_with_table
from models.expectation import EventCommon, EventSQL
from utils.logging import AppLogger

logger = AppLogger.__call__().get_logger()

router = APIRouter(prefix="/expectation", tags=["Expectations"])


# TODO: Сделать поднятие страницы index.html
# TODO: Вынести общие куски проверок в отдельный модуль/файл
# TODO: Сделать отправку результатов по почте


# TODO: Сделать вывод списка доступных проверок для данных, проблема в том что надо указывать таблицу
# @router.get("/list_available_expectation_types")
# def list_available_expectation_types(request: Request):
#     _gx = request.app.state.gx
#     avails = _gx.context.get_available_data_asset_names()
#
#     for k, v in avails.items():
#         try:
#             print('-------------------')
#             print(k, v.get('connector_postgresql', v.get('connector_vertica', None))[0])
#             print('-------------------')
#             _validator = _gx.context.get_validator(
#                 datasource_name=k,
#                 data_asset_name=v.get('connector_postgresql', v.get('connector_vertica', None))[0],
#             )
#             try:
#                 return eval(f"_validator.list_available_expectation_types()")
#             except InvalidExpectationConfigurationError as e:
#                 return e.__dict__
#         except Exception as errr:
#             print('-------------------')
#             print(errr)
#             print('-------------------')
#             continue
#     return {"request_status": "failed",
#             "dq_status": "failed",
#             "message": "There was an error while listing available expectation types"}
#
#
# # TODO: Сделать вывод документации по проверке, проблема в том что надо указывать таблицу
# @router.get(
#     "/doc_expectation/{table}/{expectation_type}", response_class=PlainTextResponse
# )
# def doc_expectation(request: Request,
#                     table: str,
#                     expectation_type: str,
#                     ):
#     _gx = request.app.state.gx
#
#     _gx.set_asset(table_name=table)
#
#     _validator = _gx.context.get_validator(
#         datasource_name=settings.sql_datasource_name,
#         data_asset_name=_gx.sql_table_asset.name,
#     )
#
#     try:
#         _doc = eval(f"_validator.{expectation_type}.__doc__")
#         return _doc
#     except Exception as e:
#         logger.error(f"Error: {e}")
#         return e.__dict__


@router.post("/run_sql_expectation")
def run_sql_expectation(
        request: Request,
        gx_mapping: EventSQL = Depends(),
        files: List[UploadFile] = File(...)
):
    _gx = request.app.state.gx
    _gx.gx_session_config = gx_mapping

    datasource = _gx.context.sources.add_or_update_sql(
        name=_gx.datasource_name + '.sql',
        connection_string=settings.sql_url.__str__()
    )
    for file in files:
        try:
            sql_query = file.file.read()
        except Exception:
            return {"request_status": "failed",
                    "dq_status": "failed",
                    "message": "There was an error uploading the file"}

        query_asset = datasource.add_query_asset(
            name=_gx.table_name + '.sql',
            query=sql_query
        )
        my_batch_request = query_asset.build_batch_request()
        _gx.context.add_or_update_expectation_suite(_gx.expectation_suite_name + '.sql')
        validator = _gx.context.get_validator(
            batch_request=my_batch_request,
            expectation_suite_name=_gx.expectation_suite_name + '.sql',
        )
        columns = validator.columns()
        print("columns", columns)
        validator.expect_table_row_count_to_equal(0, result_format={"result_format": "COMPLETE"}, include_config=True)
        for col in columns:
            validator.expect_column_values_to_be_null(col, result_format={"result_format": "COMPLETE"})
        # validator.expect_table_row_count_to_be_between(min_value=0,max_value=0,result_format={"result_format":"COMPLETE"})
        validator.save_expectation_suite(discard_failed_expectations=False)
        # print(validator.head())
        checkpoint = _gx.context.add_or_update_checkpoint(
            name=_gx.checkpoint_name + '.sql',
            validations=[
                {
                    "batch_request": my_batch_request,
                    "expectation_suite_name": validator.expectation_suite_name,
                }
            ],
        )

        checkpoint_result = _gx.context.run_checkpoint(checkpoint_name=_gx.checkpoint_name + '.sql',
                                                       run_time=datetime.now(tz=timezone('Europe/Moscow')),
                                                       result_format="COMPLETE"
                                                       )
        _gx.context.build_data_docs()
        validation_result = checkpoint_result['run_results'][next(iter(checkpoint_result['run_results']))][
            'validation_result']
        validation_success = validation_result['success']
        results = validation_result['results']

        print(checkpoint_result)
        print('-----------')
        # print(results)
        rows = []
        for result in results:
            # Define a dictionary to store the row data
            row = {'expectation_type': result['expectation_config']['expectation_type'],
                   'success': result['success']}

            for key, value in result['result'].items():
                row[key] = value

            rows.append(row)

        for exp in rows:
            expectation_type = exp.get('expectation_type')
            unexpected_index_list = exp.get('unexpected_index_list')
            df_email = pd.DataFrame.from_dict(unexpected_index_list)
            if df_email.shape[0]:
                print(df_email.shape[0], df_email)
                message_header = 'Обнаружены следующие ошибки в DQ: '
                message = make_message_with_table(data=df_email
                                                  , message_header=message_header)
                email = open(f"{SCRIPT_DIR}/outputs/email_message.html", "w")
                email.write(message)  # Adding input data to the HTML file
                email.close()  # Saving the data into the HTML file
                # send_email(
                #     to=["Turakulov.A@citilink.ru"],
                #     subject="Data Quality",
                #     html_content=message,
                # )

        return {"request_status": "success", "dq_status": validation_success}


@router.post("/run_common_expectation")
def run_common_expectation(
        request: Request,
        gx_mapping: EventCommon = Body(
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

        if isinstance(col_arg, str) and col_arg == 'all':
            col_arg = table_columns
        elif isinstance(col_arg, list):
            column_check = all(item in table_columns for item in col_arg)
            if not column_check:
                raise ValueError("Some columns listed in DQ config doesn't exist in table")
        elif isinstance(col_arg, dict):
            raise TypeError("Dictionaries in DQ config are not supported!")

        for col in col_arg:
            _kwargs = expectations_kwargs.copy()
            try:
                if 'column' in _kwargs:
                    del _kwargs['column']
                else:
                    raise KeyError
            except KeyError:
                logger.error(f'There is no column parameter in expectation config: {expectation_key}')
                return {"request_status": "failed", "dq_status": "failed"}
            _kwargs["column"] = col

            expectation_config = ExpectationConfiguration(expectation_type=expectation_key, kwargs=_kwargs)
            expectation_suite.add_expectation(expectation_config)

            # print(self._expectation_suite.show_expectations_by_expectation_type())
    _gx.context.save_expectation_suite(expectation_suite)
    _gx.context.add_or_update_checkpoint(**yaml.load(_gx.get_checkpoint_config()))

    checkpoint_result = _gx.context.run_checkpoint(checkpoint_name=_gx.checkpoint_name,
                                                   run_time=datetime.now(tz=timezone('Europe/Moscow')),
                                                   # result_format="COMPLETE"
                                                   )
    _gx.context.build_data_docs()
    validation_result = checkpoint_result['run_results'][next(iter(checkpoint_result['run_results']))][
        'validation_result']
    validation_success = validation_result['success']
    results = validation_result['results']

    print(checkpoint_result)
    print('-----------')
    # print(results)
    rows = []
    for result in results:
        # Define a dictionary to store the row data
        row = {'expectation_type': result['expectation_config']['expectation_type'],
               'success': result['success']}

        for key, value in result['result'].items():
            row[key] = value

        rows.append(row)

    for exp in rows:
        expectation_type = exp.get('expectation_type')
        unexpected_index_list = exp.get('unexpected_index_list')
        df_email = pd.DataFrame.from_dict(unexpected_index_list)
        if df_email.shape[0]:
            print(df_email.shape[0], df_email)
            message_header = 'Обнаружены следующие ошибки в DQ: '
            message = make_message_with_table(data=df_email
                                              , message_header=message_header)
            email = open(f"{SCRIPT_DIR}/outputs/email_message.html", "w")
            email.write(message)  # Adding input data to the HTML file
            email.close()  # Saving the data into the HTML file
            # send_email(
            #     to=["Turakulov.A@citilink.ru"],
            #     subject="Data Quality",
            #     html_content=message,
            # )

    return {"request_status": "success",
            "dq_status": validation_success}
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
