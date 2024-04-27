from typing import Union
from pydantic import BaseModel
from fastapi import FastAPI
import pydantic

from routers.expectation import router as gx_router

from config import settings
from service import GxSession
from utils.logging import AppLogger

logger = AppLogger.__call__().get_logger()

app = FastAPI()

app.include_router(gx_router)

@app.on_event("startup")
def startup_event():
    logger.info("Starting up...")
    logger.info(f"Connecting to database...{settings.sql_url.__str__()}")
    app.state.gx = GxSession(
        settings.sql_url.__str__(),
        settings.sql_datasource_name,
    )
