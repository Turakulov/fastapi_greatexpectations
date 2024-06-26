import os

from pydantic_core import Url
from pydantic_settings import BaseSettings

from dotenv import load_dotenv

load_dotenv('.env')

SCRIPT_DIR = os.path.dirname(__file__)


class SqlDBUrl(Url):
    allowed_schemes = {
        "postgresql",
    }
    user_required = True
    password_required = True
    host_required = True


class Settings(BaseSettings):
    sql_url: SqlDBUrl = SqlDBUrl.build(
        scheme="postgresql",
        username=os.getenv("SQL_USER"),
        password=os.getenv("SQL_PASSWORD"),
        host=os.getenv("SQL_HOST"),
        port=int(os.getenv("SQL_PORT")),
        path=f"{os.getenv('SQL_DB') or ''}"
    )


settings = Settings()
