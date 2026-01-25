# src/infrastructure/kafka.py

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition, ConsumerRecord
from returns.future import future_safe
from returns.result import  safe
from structlog.typing import FilteringBoundLogger
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError


from domain.interfaces import KafkaProvider


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

    @safe
    def subscribe(self, topics: list[str]) -> None:
        """
        Subscribe to new topics dynamically. 
        This method updates the internal consumer subscription.
        """
        # Pass the topics tuple/iterable to the underlying consumer
        return self._consumer.subscribe(topics)

    @future_safe
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

    @future_safe
    async def send(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None
    ) -> bool:
        """Consumers cannot send messages."""
        raise RuntimeError("Send not supported by consumer")


@dataclass(slots=True, frozen=True)
class KafkaProducerAdapter(KafkaProvider):
    bootstrap_servers: str
    logger: FilteringBoundLogger

    @cached_property
    def _producer(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)

    @future_safe
    async def send(
            self,
            topic: str,
            value: bytes,
            key: bytes | None = None
    ) -> bool:
        """
        Send a message to the specified topic.
        Uses the cached producer instance for efficiency.
        """
        try:
            # Note: For AIOKafkaProducer, it is best practice to start/stop
            # the producer around operations, or manage it as a context manager.
            # Our dependency container manages the init and cleanup so no need for it here.
            await self._producer.send_and_wait(topic, value, key=key)
            return True

        except Exception as e:
            raise e

    @safe
    def subscribe(self, *topics: list[str]) -> None:
        """Producer does not support subscription."""
        pass

    @future_safe
    async def getmany(
        self,
        *partitions: Any,
        timeout_ms: int | float = 0,
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[ConsumerRecord]]:
        """Producer does not support consumption."""
        return {}

    @future_safe
    async def ensure_topics_exist(
        self, 
        topics: list[str], 
        num_partitions: int = 1, 
        replication_factor: int = 1
    ) -> list[str]:
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id='admin-client'
        )
        
        try:
            new_topics: list[NewTopic] = [
                NewTopic(
                    name=topic, 
                    num_partitions=num_partitions, 
                    replication_factor=replication_factor
                ) 
                for topic in topics
            ]
            
            # This call is synchronous in aiokafka's admin client
            await admin_client.create_topics(new_topics=new_topics, validate_only=False)
            return topics
            
        except TopicAlreadyExistsError:
            return topics
        except Exception as e:
            raise e
        finally:
            await admin_client.close()




