from datetime import date
from logging import getLogger
from typing import final

from sqlalchemy import (
    Date,
    Double,
    PrimaryKeyConstraint,
    String,
    select,
    )
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from server.models import CargoType, Tariff

logger = getLogger(__name__)


class BaseTable(DeclarativeBase):
    """
    Base class for all database models.
    """


class TariffTable(BaseTable):
    __tablename__ = 'tariffs'

    date: Mapped[date] = mapped_column(Date, nullable=False)
    cargo_type: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    rate: Mapped[float] = mapped_column(Double, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(date, cargo_type, name='unique_date_cargo_type'),
        )

    def to_model(self, /) -> Tariff:
        """
        Converts this row to a model instance.
        """
        return Tariff.model_validate(self)


@final
class DatabaseRequester:
    """
    A class for querying the database.
    """
    __slots__ = '_session',

    def __init__(self, session: AsyncSession, /) -> None:
        self._session = session

    async def fetch_cargo_type(self, cargo_type_name: RawCargoType, /) -> CargoType | None:
        """
        Fetches ID for the given cargo type name,
        then returns :class:`CargoType` or ``None`` if no such cargo type exists.
        """
        query = select(CargoTypeTable).where(CargoTypeTable.name == cargo_type_name)
        result = await self._session.scalars(query)
        row = await result.first()
        return CargoType.model_validate(row) if row else None

    async def add_cargo_types(
            self,
            cargo_type_name: RawCargoType,
            /,
            *cargo_type_names: RawCargoType,
            ) -> list[CargoType]:
        """
        Adds specified cargo types to the database if they are not present.
        Returns the list of added cargo types.
        """
        vals = [dict(name=cargo_type_name)]
        vals.extend(dict(name=name) for name in cargo_type_names)

        query = (
            insert(CargoTypeTable)
            .values(vals)
            .on_conflict_do_nothing()
            .returning(CargoTypeTable)
        )
        result = await self._session.scalars(query)
        li = [CargoType.model_validate(row) for row in result]
        for row in li:
            logger.info(f'Added cargo type {row}')
            # todo log all added rows to kafka

        return li

    async def fetch_tariff(
            self,
            ensurance_date: date,
            cargo_type: CargoType,
            /
            ) -> Tariff | None:
        """
        Fetches tariff for the given date and cargo type,
        then returns :class:`Tariff` or ``None`` if no such tariff exists.
        """
        query = (
            select(TariffTable)
            .where(TariffTable.date == ensurance_date)
            .where(TariffTable.cargo_type == cargo_type.id)

        )
        result = await self._session.scalars(query)
        row = await result.first()
        return Tariff(date=ensurance_date, cargo_type=cargo_type, rate=row.rate) if row else None


__all__ = 'BaseTable', 'DatabaseRequester'
