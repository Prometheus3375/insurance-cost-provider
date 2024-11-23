from collections import defaultdict
from datetime import date
from typing import Annotated

from pydantic import (
    AfterValidator,
    BaseModel,
    NonNegativeInt,
    PositiveFloat,
    StringConstraints,
    )
from pydantic_settings import BaseSettings

type NonEmptyString = Annotated[str, StringConstraints(min_length=1)]
type CargoType = NonEmptyString


class Tariff(BaseModel, frozen=True):
    """
    Model for tariffs of a certain date.
    """
    cargo_type: CargoType
    rate: PositiveFloat


def _format_index_list(indexes: list[int], /) -> str:
    return f'{', '.join(map(str, indexes[:-1]))} and {indexes[-1]}'


def validate_tariff_list(li: list[Tariff], /) -> list[Tariff]:
    """
    Validates that a tariff list has one instance of :class:`Tariff` per cargo type.
    """
    ct2indexes = defaultdict(list)
    for i, tariff in enumerate(li):
        ct2indexes[tariff.cargo_type].append(i)

    errors = [
        f'at indexes {_format_index_list(indexes)} '
        f'share the same cargo type {cargo_type!r}'
        for cargo_type, indexes in ct2indexes.items()
        if len(indexes) > 1
        ]
    if errors:
        raise ValueError(f'tariffs {'; '.join(errors)}')

    return li


type TariffList = Annotated[list[Tariff], AfterValidator(validate_tariff_list)]
type TariffData = dict[date, TariffList]


class Settings(BaseSettings, env_ignore_empty=True):
    """
    Model for holding server settings.
    """
    database_uri: NonEmptyString
    connection_pool_min_size: NonNegativeInt = 1
    connection_pool_max_size: NonNegativeInt | None = None

    kafka_uri: NonEmptyString


__all__ = (
    'NonEmptyString',
    'CargoType',
    'Tariff',
    'TariffList',
    'TariffData',
    'Settings',
    )
