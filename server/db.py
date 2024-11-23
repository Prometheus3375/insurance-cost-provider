from logging import getLogger
from typing import final

from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict

logger = getLogger(__name__)


async def configure_connection(conn: AsyncConnection, /) -> None:
    """
    A function for configuring a database connection.
    """
    await conn.set_autocommit(True)
    await conn.set_read_only(True)


async def verify_connectivity(uri: str, /) -> bool:
    """
    Verifies whether a connection to the database with the given URI can be established
    and logs errors if it is not possible.
    """
    try:
        parsed = conninfo_to_dict(uri)
        logger.info(
            f"Verifying connectivity to the database "
            f"postgresql://{parsed['user']}:REDACTED@"
            f"{parsed['host']}:{parsed['port']}/{parsed['dbname']}"
            )
    except Exception:
        logger.exception('Failed to parse the provided PostgreSQL URI')
        return False

    try:
        async with await AsyncConnection.connect(uri) as conn:
            await configure_connection(conn)
            logger.info('Connection to the database can be established successfully')
    except Exception:
        logger.exception('Connection to the database cannot be established')
        return False

    return True


@final
class PostgreSQLRequester:
    """
    A class for querying PostgreSQL database.
    """
    __slots__ = '_conn',

    def __init__(self, connection: AsyncConnection, /) -> None:
        self._conn = connection


__all__ = 'PostgreSQLRequester', 'configure_connection', 'verify_connectivity'
