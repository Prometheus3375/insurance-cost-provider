from datetime import date
from logging import getLogger
from typing import final

from sqlalchemy import (
    Date,
    Double,
    PrimaryKeyConstraint,
    String,
    delete,
    select,
    update,
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
    cargo_type: Mapped[str] = mapped_column(String(50), nullable=False)
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

    async def fetch_tariff(
            self,
            /,
            *,
            tariff_date: date,
            cargo_type: CargoType,
            ) -> Tariff | None:
        """
        Fetches tariff for the given date and cargo type,
        then returns :class:`Tariff` or ``None`` if no such tariff exists.
        """
        query = (
            select(TariffTable)
            .where(
                TariffTable.date == tariff_date,
                TariffTable.cargo_type == cargo_type,
                )
        )
        result = (await self._session.scalars(query)).first()
        return result.to_model() if result else None

    async def add_tariffs(
            self,
            tariff: Tariff,
            /,
            *tariffs: Tariff,
            ) -> list[Tariff]:
        """
        Adds specified tariffs to the database if they are not present
        and edits existing tariffs when necessary.
        Returns the list of added and updated tariffs.
        """
        vals = [tariff.model_dump()]
        vals.extend(t.model_dump() for t in tariffs)

        query = insert(TariffTable).values(vals)
        query = (
            query.on_conflict_do_update(
                constraint='unique_date_cargo_type',
                set_=dict(rate=query.excluded.rate),
                where=TariffTable.rate != query.excluded.rate,
                )
            .returning(TariffTable)
        )
        result = await self._session.scalars(query)
        li = [row.to_model() for row in result]
        for t in li:
            logger.info(f'Added or updated tariff {t}')
            # todo log all added rows to kafka

        return li

    async def edit_tariff(self, tariff: Tariff, /) -> Tariff | None:
        """
        Edits specified tariff via updating its rate value.
        Returns the updated tariff or ``None``
        if such tariff does not exist, or it already has such value for rate.
        """
        query = (
            update(TariffTable)
            .where(
                TariffTable.date == tariff.date,
                TariffTable.cargo_type == tariff.cargo_type,
                TariffTable.rate != tariff.rate,
                )
            .values(dict(rate=tariff.rate))
            .returning(TariffTable)
        )
        result = (await self._session.scalars(query)).first()
        if result:
            new_tariff = result.to_model()
            logger.info(f'Edited tariff {new_tariff}')
            # todo log action to kafka
            return new_tariff

        return None

    async def delete_tariff(
            self,
            /,
            *,
            tariff_date: date,
            cargo_type: CargoType,
            ) -> Tariff | None:
        """
        Deletes tariff for the given date and cargo type.
        Returns the deleted tariff or ``None``
        if such tariff does not exist.
        """
        query = (
            delete(TariffTable)
            .where(
                TariffTable.date == tariff_date,
                TariffTable.cargo_type == cargo_type,
                )
            .returning(TariffTable)
        )
        result = (await self._session.scalars(query)).first()
        if result:
            tariff = result.to_model()
            logger.info(f'Deleted tariff {tariff}')
            # todo log action to kafka
            return tariff

        return None


__all__ = 'BaseTable', 'DatabaseRequester'
