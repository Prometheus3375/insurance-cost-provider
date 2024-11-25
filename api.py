from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import date
from logging import getLogger
from typing import Annotated

from fastapi import Body, Depends, FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import HTTPException, RequestValidationError
from pydantic import PositiveFloat
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from starlette import status
from starlette.responses import JSONResponse

from server.db import *
from server.models import *

settings: Settings
engine: AsyncEngine
session_maker: async_sessionmaker


@asynccontextmanager
async def lifespan(_: FastAPI, /) -> Iterator[None]:
    # Actions on startup
    from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

    global settings, engine, session_maker
    settings = Settings()

    engine = create_async_engine(
        settings.database_uri,
        # connect_args=dict(autocommit=True),
        pool_size=settings.pool_size,
        max_overflow=settings.pool_overflow,
        pool_recycle=settings.pool_recycle,
        pool_timeout=settings.pool_timeout,
        )

    session_maker = async_sessionmaker(engine, autobegin=False)

    logger.info(f'Verifying connectivity to the database {settings.database_redacted_uri}')
    connection: AsyncConnection
    async with engine.begin() as connection:
        logger.info('Connection to the database can be established successfully')
        # Ensure tables exist
        await connection.run_sync(BaseTable.metadata.create_all)

    yield
    # Actions on shutdown
    await engine.dispose()


async def make_db_requester() -> Iterator[DatabaseRequester]:
    """
    Dependency function for creating an instance of :class:`DatabaseRequester`.
    """
    async with session_maker.begin() as session:
        yield DatabaseRequester(session)


# https://github.com/fastapi/fastapi/issues/10719
DBRequester = Annotated[DatabaseRequester, Depends(make_db_requester)]


async def verify_cargo_type(
        *,
        db_requester: DBRequester,
        cargo_type: RawCargoType,
        ) -> CargoType:
    """
    Dependency function for verifying passed cargo types.
    """
    result = await db_requester.fetch_cargo_type(cargo_type)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f'Cargo type {cargo_type!r} not found',
            )

    return result


type ValidCargoType = Annotated[CargoType, Depends(verify_cargo_type)]
logger = getLogger(__name__)
app = FastAPI(
    title='Cost Evaluation API',
    version='1.0.0',
    lifespan=lifespan,
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
        request: Request,
        exc: RequestValidationError,
        /,
        ) -> JSONResponse:
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
        db_requester: DBRequester,
        ensurance_date: date,
        cargo_type: ValidCargoType,
        declared_price: PositiveFloat,
        ) -> PositiveFloat:
    """
    todo
    """
    tariff = await db_requester.fetch_tariff(ensurance_date, cargo_type)
    if tariff is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f'Tariff for {cargo_type!r} on {ensurance_date} not found',
            )

    return tariff.rate * declared_price


@app.get('/api/internal/cargo-type/list')
async def api_add_cargo_type(
        *,
        db_requester: DBRequester,
        ) -> list[CargoType]:
    """
    todo
    """


@app.get('/api/internal/cargo-type/add/{cargo_type}')
async def api_add_cargo_type(
        *,
        db_requester: DBRequester,
        cargo_type: RawCargoType,
        ) -> str:
    """
    todo
    """
    result = await db_requester.add_cargo_types(cargo_type)
    return 'Success' if result else 'Already exists'


@app.get('/api/internal/cargo-type/delete/{cargo_type}')
async def api_delete_cargo_type(
        *,
        db_requester: DBRequester,
        cargo_type: ValidCargoType,
        ) -> None:
    """
    todo
    """
    # todo log who did that to kafka


@app.post('/api/internal/tariffs/load')
async def api_load_tariffs(
        *,
        db_requester: DBRequester,
        data: Annotated[PlainTariffData, Body()],
        ) -> None:
    """
    todo
    """
    # todo log who did that to kafka
    # todo use edit and add for this


@app.get('/api/internal/tariffs/edit')
async def api_edit_tariff(
        *,
        db_requester: DBRequester,
        tariff_date: date,
        cargo_type: ValidCargoType,
        new_rate: PositiveFloat,
        ) -> None:
    """
    todo
    """
    # todo log who did that to kafka


@app.get('/api/internal/tariffs/delete')
async def api_delete_tariff(
        *,
        db_requester: DBRequester,
        tariff_date: date,
        cargo_type: ValidCargoType,
        ) -> None:
    """
    todo
    """
    # todo log who did that to kafka
