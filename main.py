from typing import Annotated
from fastapi import FastAPI
from fastapi import File, UploadFile

from routers.expectation import router as gx_router

from service import GxSession
from utils.logging import AppLogger

logger = AppLogger.__call__().get_logger()

app = FastAPI()

app.include_router(gx_router)





@app.on_event("startup")
def startup_event():
    logger.info("Starting up...")
    app.state.gx = GxSession()
