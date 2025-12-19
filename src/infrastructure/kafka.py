# src/infrastructure/kafka_connector.py

from dataclasses import dataclass
from aiokafka import AIOKafkaProducer
from returns.result import Result, Success, Failure
from structlog.typing import FilteringBoundLogger
from ..domain.interfaces import KafkaProvider


@dataclass(slots=True, frozen=True)
class KafkaConnector(KafkaProvider):
    _producer: AIOKafkaProducer
    _logger: FilteringBoundLogger

    async def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None
    ) -> Result[bool, Exception]:
        try:
            await self._producer.send_and_wait(topic, value, key=key)
            return Success(True)
        except Exception as e:
            self._logger.error("kafka_produce_failed",
                               error=str(e), topic=topic)
            return Failure(e)
