from json import JSONEncoder
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.producer.message_accumulator import BatchBuilder

from server.models import Settings

_json_encoder = JSONEncoder(
    skipkeys=False,
    ensure_ascii=False,
    check_circular=True,
    allow_nan=True,
    sort_keys=True,
    indent=None,
    separators=(',', ':'),
    )


def compact_json(obj: Any, /) -> bytes:
    """
    Encodes an object using compact JSON encoder.
    """
    return _json_encoder.encode(obj).encode()


class Broker:
    """
    Broker for sending logs about database operations.
    """
    __slots__ = '_settings', '_producer', '_batch'

    def __init__(self, settings: Settings, producer: AIOKafkaProducer, /) -> None:
        self._settings = settings
        self._producer = producer
        self._batch: BatchBuilder = producer.create_batch()

    async def log(self, /, user: str, operation: str, msg: str) -> None:
        """
        Forms a message about database operation and adds it to the batch.
        """
        data = dict(user=user, operation=operation, message=msg)
        result = self._batch.append(
            timestamp=None,  # current time
            key=None,
            value=compact_json(data),
            )
        if result is None:
            await self.flush()
            self._batch = self._producer.create_batch()

    async def flush(self, /) -> None:
        """
        Flushes the current batch to the server.
        """
        self._batch.close()
        await self._producer.send_batch(self._batch, self._settings.kafka_topic, partition=0)


__all__ = 'compact_json', 'Broker'
