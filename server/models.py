from collections import defaultdict
from datetime import date
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    StringConstraints,
    )
from pydantic_settings import BaseSettings

type NonEmptyString = Annotated[str, StringConstraints(min_length=1)]


class Settings(BaseSettings, env_ignore_empty=True):
    """
    Model for holding server settings.
    """
    database_user: NonEmptyString
    database_password: NonEmptyString
    database_host: NonEmptyString
    database_port: PositiveInt
    database_dbname: NonEmptyString

    pool_size: NonNegativeInt = 5
    pool_overflow: NonNegativeInt = 10
    pool_recycle: PositiveFloat | Literal[-1] = 3600
    pool_timeout: PositiveFloat = 30

    @property
    def database_uri(self, /) -> str:
        """
        URI of the database.
        """
        return (
            f'postgresql+psycopg://{self.database_user}:{self.database_password}@'
            f'{self.database_host}:{self.database_port}/{self.database_dbname}'
        )

    @property
    def database_redacted_uri(self, /) -> str:
        """
        URI of the database with password redacted.
        """
        return (
            f'postgresql+psycopg://{self.database_user}:REDACTED@'
            f'{self.database_host}:{self.database_port}/{self.database_dbname}'
        )


type CargoType = Annotated[str, StringConstraints(min_length=1, max_length=50)]


class BaseTariff(BaseModel, frozen=True):
    """
    Model for tariffs without specified date and rate.
    """
    cargo_type: CargoType


class PlainTariff(BaseTariff, frozen=True):
    """
    Model for tariffs without a date.
    """
    rate: PositiveFloat


class TariffType(BaseTariff, frozen=True):
    """
    Model for tariffs without specified rate.
    """
    date: date


class Tariff(TariffType, frozen=True, from_attributes=True):
    """
    Model for tariffs.
    """
    rate: PositiveFloat


def validate_tariff_list(li: list[PlainTariff], /) -> list[PlainTariff]:
    """
    Validates that a tariff list has one instance of :class:`PlainTariff` per cargo type.
    """
    ct2indexes = defaultdict(list)
    for i, tariff in enumerate(li):
        ct2indexes[tariff.cargo_type].append(i)

    errors = [
        f'at indexes {', '.join(map(str, indexes[:-1]))} and {indexes[-1]} '
        f'share the same cargo type {cargo_type!r}'
        for cargo_type, indexes in ct2indexes.items()
        if len(indexes) > 1
        ]
    if errors:
        raise ValueError(f'tariffs {'; '.join(errors)}')

    return li


type PlainTariffList = Annotated[list[PlainTariff], AfterValidator(validate_tariff_list)]
type PlainTariffData = dict[date, PlainTariffList]

__all__ = (
    'NonEmptyString',
    'RawCargoType',
    'CargoType',
    'Tariff',
    'PlainTariff',
    'PlainTariffList',
    'PlainTariffData',
    'Settings',
    )
