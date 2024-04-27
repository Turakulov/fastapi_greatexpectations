from pydantic import BaseModel, ConfigDict


class Event(BaseModel):
    model_config = ConfigDict(strict=True)

    source: str
    table_name: str
    schema_name: str
    connection: str
    expectations: dict

