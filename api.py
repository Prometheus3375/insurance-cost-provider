from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import date
from logging import getLogger
from typing import Annotated

from fastapi import Depends, FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from psycopg_pool import AsyncConnectionPool
from pydantic import PositiveFloat

from server.db import *
from server.models import *

settings: Settings
connection_pool: AsyncConnectionPool


@asynccontextmanager
async def lifespan(_: FastAPI, /) -> Iterator[None]:
    global settings, connection_pool
    # Actions on startup
    settings = Settings()

    if not (await verify_connectivity(settings.database_uri)):
        raise ValueError('environmental variable DATABASE_URI is not properly set')

    connection_pool = AsyncConnectionPool(
        settings.database_uri,
        kwargs=None,
        min_size=settings.connection_pool_min_size,
        max_size=settings.connection_pool_max_size,
        open=False,
        configure=configure_connection,
        check=AsyncConnectionPool.check_connection,
        name='Global PostgreSQL connection pool',
        )

    await connection_pool.open()
    await connection_pool.wait()
    logger.info(
        f'Connection pool is ready. '
        f'Min size is {connection_pool.min_size}, '
        f'max size is {connection_pool.max_size}'
        )
    yield
    # Actions on shutdown
    await connection_pool.close()


async def make_db_requester() -> Iterator[PostgreSQLRequester]:
    """
    Dependency function for creating an instance of :class:`PostgreSQLRequester`.
    """
    async with connection_pool.connection() as conn:
        yield PostgreSQLRequester(conn)


type DBRequesterType = Annotated[PostgreSQLRequester, Depends(make_db_requester)]
app = FastAPI(
    title='Cost Evaluation API',
    version='1.0.0',
    lifespan=lifespan,
    )
logger = getLogger(__name__)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError, /):
    """
    Request validation handler which logs errors caused by request validation in detail.
    """
    errors = []
    for d in exc.errors():
        msg = d['msg']
        loc = '.'.join(str(part) for part in d['loc'])  # some parts can be integers
        errors.append(f'At location {loc!r} {msg[0].lower()}{msg[1:]}')

    err_noun = 'error' if len(errors) == 1 else 'errors'
    err_msgs = '\n  '.join(errors)
    logger.error(f'{len(errors)} validation {err_noun} in the recent request:\n  {err_msgs}')
    return await request_validation_exception_handler(request, exc)


@app.get('/api/public/evaluate_cost')
async def api_evaluate_cost(
        *,
        db_requester: DBRequesterType,
        ensurance_date: date,
        cargo_type: CargoType,
        declared_price: PositiveFloat,
        ) -> PositiveFloat:
    """
    todo
    """
    # todo request rate by cargo_type and ensurance date
    rate = 1
    return rate * declared_price


@app.get('/api/internal/{cargo_type}/add')
async def api_add_cargo_type(
        *,
        db_requester: DBRequesterType,
        cargo_type: CargoType,
        ) -> None:
    """
    todo
    """
    # todo log who did that to kafka


@app.get('/api/internal/{cargo_type}/delete')
async def api_delete_cargo_type(
        *,
        db_requester: DBRequesterType,
        cargo_type: CargoType,
        ) -> None:
    """
    todo
    """
    # todo log who did that to kafka


@app.post('/api/internal/tariffs/load')
async def api_load_tariffs(
        *,
        db_requester: DBRequesterType,
        data: TariffData,
        ) -> None:
    """
    todo
    """
    # todo log who did that to kafka
    # todo use edit and add for this


@app.get('/api/internal/tariffs/edit')
async def api_edit_tariff(
        *,
        db_requester: DBRequesterType,
        tariff_date: date,
        cargo_type: CargoType,
        new_rate: PositiveFloat,
        ) -> None:
    """
    todo
    """
    # todo log who did that to kafka


@app.get('/api/internal/tariffs/delete')
async def api_delete_tariff(
        *,
        db_requester: DBRequesterType,
        tariff_date: date,
        cargo_type: CargoType,
        ) -> None:
    """
    todo
    """
    # todo log who did that to kafka
