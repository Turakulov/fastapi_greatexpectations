from typing import Union, List

import pandas as pd

from config import SCRIPT_DIR
from utils.logging import AppLogger

logger = AppLogger.__call__().get_logger()


def read_file(path: str, encoding: str = 'utf-8') -> str:
    """Function for reading files with various file format. First of all
        for files with '.sql' and '.py' extentions

    Args:
        path (str): full path to file with extention
        encoding (str, optional): File encoding. Defaults to 'utf-8'.

    Returns:
        str: file content
    """
    with open(file=path, mode='r', encoding=encoding) as fr:
        meta = fr.read()
    return meta


def make_message_with_table(data: Union[List[List] | pd.DataFrame]
                            , message_header: str
                            , column_list=None
                            ) -> str:
    """Function for message generation.
    Message contains table with content.

    Args:
        data (List[List]): Table content
        column_list (List): Table columns list
        message_header (str): The message header

    Raises:
        ValueError: Raises if the column_list parameter doesn't contain data

    Returns:
        str: _description_
    """
    if column_list is None:
        column_list = []
    if isinstance(data, pd.DataFrame):
        column_list = data.columns.tolist()
        data = data.values.tolist()
    elif not column_list:
        raise ValueError('Отсутствует список с полями таблицы')

    message = ''
    if data:
        mail_text = ''
        logger.info(f'Всего выгружено строк из БД: {len(data)}')
        for row in data:
            mail_text += '<tr>'
            for row_value in row:
                mail_text += '<td>' + str(row_value) + '</td>'
            mail_text += '</tr>'

        table_header = ''
        for col in column_list:
            table_header += '<th class="table__heading">' + str(col) + '</th>'

        message = read_file(f"{SCRIPT_DIR}/templates/table_template.txt")
        message = message.replace('*TABLE_ROWS*', mail_text) \
            .replace('*MESSAGE_HEADER*', message_header) \
            .replace('*TABLE_HEADER*', table_header)
        logger.info('Данные получены, таблица сформирована')
    else:
        logger.info('Данные отсутcтвуют, таблица не сформирована')

    return message


