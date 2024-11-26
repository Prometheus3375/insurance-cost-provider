from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import date
from logging import getLogger
from typing import Annotated

from fastapi import Body, Depends, FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import PositiveFloat
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from starlette import status

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
DateInBody = Annotated[date, Body(embed=True)]
CargoTypeInBody = Annotated[CargoType, Body(embed=True)]
PositiveFloatInBody = Annotated[PositiveFloat, Body(embed=True)]

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


ResponseTariffNotFound = {404: dict(description='Tariff not found', model=SimpleResponse)}
ResponseTariffNotModified = {
    304: dict(
        description='Tariff unchanged or not found',
        model=None,
        ),
    }


@app.post('/api/public/evaluate_cost', responses=ResponseTariffNotFound)
async def api_evaluate_cost(
        *,
        db_requester: DBRequester,
        ensurance_date: DateInBody,
        cargo_type: CargoTypeInBody,
        declared_price: PositiveFloatInBody,
        ) -> PositiveFloat:
    """
    Evaluates cost using specified date, cargo type and declared price.
    Returns status 404 if tariff for such date and cargo type does not exist.
    """
    tariff = await db_requester.fetch_tariff(
        tariff_date=ensurance_date,
        cargo_type=cargo_type,
        )

    if tariff is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f'Tariff for {cargo_type!r} on {ensurance_date} is not found',
            )

    return tariff.rate * declared_price


@app.post('/api/internal/tariffs/load')
async def api_load_tariffs(
        *,
        db_requester: DBRequester,
        data: Annotated[PlainTariffData, Body()],
        ) -> list[Tariff]:
    """
    Loads tariffs to the database.
    If any tariff from the payload already exists in the database,
    then updates its rate value if new one is different.
    Returns the list of added and updated tariffs.
    """
    tariffs = (
        Tariff(date=date_, cargo_type=plain_tariff.cargo_type, rate=plain_tariff.rate)
        for date_, plain_tariff_list in data.items()
        for plain_tariff in plain_tariff_list
        )
    # noinspection PyArgumentList
    return await db_requester.add_tariffs(*tariffs)


@app.post('/api/internal/tariffs/edit', responses=ResponseTariffNotModified)
async def api_edit_tariff(
        *,
        db_requester: DBRequester,
        tariff_date: DateInBody,
        cargo_type: CargoTypeInBody,
        new_rate: PositiveFloatInBody,
        ) -> SimpleResponse:
    """
    Edits tariff with a new value of rate.
    Returns status 304 if value is identical or such tariff does not exist.
    """
    tariff = Tariff(date=tariff_date, cargo_type=cargo_type, rate=new_rate)
    result = await db_requester.edit_tariff(tariff)
    if result is None:
        raise HTTPException(status.HTTP_304_NOT_MODIFIED)

    return SimpleResponse(detail='Success')


@app.post('/api/internal/tariffs/delete', responses=ResponseTariffNotFound)
async def api_delete_tariff(
        *,
        db_requester: DBRequester,
        tariff_date: DateInBody,
        cargo_type: CargoTypeInBody,
        ) -> Tariff:
    """
    Deletes tariff specified by its date and cargo type.
    Returns status 404 if such tariff does not exist.
    """
    result = await db_requester.delete_tariff(
        tariff_date=tariff_date,
        cargo_type=cargo_type,
        )

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f'Tariff for {cargo_type!r} on {tariff_date} is not found',
            )

    return result
