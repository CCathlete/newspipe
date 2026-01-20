# src/infrastructure/kafka.py
from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition, ConsumerRecord
from returns.result import Failure, Success, Result
from structlog.typing import FilteringBoundLogger

from ..domain.interfaces import KafkaProvider


@dataclass(slots=True, frozen=True)
class KafkaConsumerAdapter(KafkaProvider):
    bootstrap_servers: str
    group_id: str
    topics: tuple[str, ...]
    logger: FilteringBoundLogger

    @cached_property
    def _consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            list(self.topics),
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

    def subscribe(self, *topics: list[str]) -> None:
        """
        Subscribe to new topics dynamically. 
        This method updates the internal consumer subscription.
        """
        # Pass the topics tuple/iterable to the underlying consumer
        self._consumer.subscribe(topics)

    async def getmany(
        self,
        *partitions: Any,
        timeout_ms: int = 0,
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[ConsumerRecord]]:
        """
        Retrieve messages from the subscribed topics.
        """
        return await self._consumer.getmany(
            timeout_ms=timeout_ms,
            max_records=max_records
        )

    async def send(self, topic: str, value: bytes, key: bytes | None = None) -> Result[bool, Exception]:
        """Consumers cannot send messages."""
        return Failure(RuntimeError("Send not supported by consumer"))


@dataclass(slots=True, frozen=True)
class KafkaProducerAdapter(KafkaProvider):
    bootstrap_servers: str
    logger: FilteringBoundLogger

    @cached_property
    def _producer(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)

    async def send(self, topic: str, value: bytes, key: bytes | None = None) -> Result[bool, Exception]:
        """
        Send a message to the specified topic.
        Uses the cached producer instance for efficiency.
        """
        try:
            # Note: For AIOKafkaProducer, it is best practice to start/stop
            # the producer around operations, or manage it as a context manager.
            # Since we are using a property, we ensure start is called here.
            await self._producer.start()
            await self._producer.send_and_wait(topic, value, key=key)
            await self._producer.stop()
            return Success(True)
        except Exception as exc:
            return Failure(exc)

    def subscribe(self, *topics: list[str]) -> None:
        """Producer does not support subscription."""
        pass

    async def getmany(
        self,
        *partitions: Any,
        timeout_ms: int | float = 0,
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[ConsumerRecord]]:
        """Producer does not support consumption."""
        return {}
