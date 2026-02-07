import asyncio
import logging
import contextlib

from fastapi import Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError


logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s %(asctime)s %(name)s %(message)s'
)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.core.settings import settings
from src.api.v1.router import api_router
from src.websocket.bridge import redis_to_ws_bridge


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    bridge_task = asyncio.create_task(redis_to_ws_bridge())
    yield
    bridge_task.cancel()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    openapi_url=f'{settings.API_VERSION}/openapi.json',
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)


@app.middleware("http")
async def unique_violation_middleware(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except IntegrityError as e:
        error_msg = str(e.orig).lower()
        if "duplicate key value" in error_msg or "unique constraint" in error_msg:
            return JSONResponse(
                status_code=status.HTTP_409_CONFLICT,
                content={"detail": "Запись с такими данными уже существует"}
            )
        raise


app.include_router(api_router, prefix=settings.API_VERSION)

