# src/domain/interfaces.py

from typing import Any, Iterable, Protocol, runtime_checkable
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from returns.result import Result, safe
from returns.future import FutureResult, future_safe
from aiokafka.structs import TopicPartition, ConsumerRecord
from .models import BronzeRecord


class SparkSessionInterface(Protocol):
    def createDataFrame(
        self,
        data: Iterable[Any],
        schema: StructType
    ) -> DataFrame: ...


class AIPort(Protocol):
    @future_safe
    async def is_relevant(
        self,
        text: str,
        policy_description: str,
        language: str = "en",
    ) -> bool:
        ...

    @future_safe
    async def embed_text(self, text: str) -> Result[list[float], Exception]:
        ...


class StoragePort(Protocol):
    @future_safe
    async def write_records(
        self,
        records: list[BronzeRecord],
    ) -> int: ...


class KafkaPort(Protocol):
    @future_safe
    async def send(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
    ) -> None:
        ...

    @safe
    def subscribe(self, topics: list[str]) -> None:
        ...

    @future_safe
    async def getmany(
        self,
        *partitions: Any,
        timeout_ms: int = 0,
        max_records: int | None = None
    ) -> dict[TopicPartition, list[ConsumerRecord[Any, Any]]]: 
        ...

    @future_safe
    async def ensure_topics_exist(
        self, 
        topics: list[str], 
        num_partitions: int = 1, 
        replication_factor: int = 1
    ) ->list[str]:
        ...


@runtime_checkable
class ChunkingStrategy(Protocol):
    window_size: int
    overlap: int

    def chunk(self, content: str) -> list[str]: 
        ...


@runtime_checkable
class CrawlerResult(Protocol):
    success: bool
    error_message: str | None
    markdown: str
    links:dict[str, list[dict[str, str]]]
    metadata: dict[str, Any]


@runtime_checkable
class CrawlerRunConfig(Protocol):
    cache_mode: Any
    chunking_strategy: ChunkingStrategy
    markdown_generator: Any
    link_extraction: bool


@runtime_checkable
class Crawler(Protocol):
    async def arun(
        self, url: str, config: CrawlerRunConfig) -> CrawlerResult: ...

    async def __aenter__(self) -> "Crawler": ...

    async def __aexit__(self, exc_type: Any, exc_val: Any,
                        exc_tb: Any) -> None: ...


class ScraperPort(Protocol):
    @future_safe
    async def deep_crawl(
        self,
        url: str,
        language: str,
        discovery_topics: list[str] = ["discovery_queue"],
        chunks_topic: str = "raw_chunks",
    ) -> None:
        """Performs crawl, discovers links, and publishes chunks to Kafka."""
        ...

    @future_safe
    async def initialize_and_seed(
            self,
            seeds: dict[str, list[str]],
            topics: list[str]
    ) -> list[str]:
        ...


class KafkaMessage(Protocol):
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    timestamp: int
