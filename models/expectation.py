from pydantic import BaseModel


class EventCommon(BaseModel):
    source: str
    table_name: str
    schema_name: str
    connection: str
    expectations: dict

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "source": "vertica",
                    "table_name": "some_table_name",
                    "schema_name": "dev",
                    "connection": "vertica",
                    "expectations": {
                        "expect_column_values_to_not_be_null": {
                            "column": "all"
                        },
                        "expect_column_to_exist": {
                            "column": "all"
                        },
                        "expect_column_proportion_of_unique_values_to_be_between": {
                            "column": [
                                "some_column"
                            ],
                            "min_value": 110,
                            "max_value": 1
                        }
                    }
                }
            ]
        }
    }


class EventSQL(BaseModel):
    source: str
    table_name: str
    schema_name: str
    connection: str

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "source": "vertica",
                    "table_name": "some_table_name",
                    "schema_name": "dev",
                    "connection": "vertica"
                }
            ]
        }
    }
