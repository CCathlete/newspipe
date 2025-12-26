# src/domain/interfaces.py

from typing import Any, Iterable, Protocol, AsyncIterator, runtime_checkable
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from returns.result import Result
from aiokafka.structs import TopicPartition, ConsumerRecord
from .models import BronzeTagResponse, BronzeRecord


class SparkSessionInterface(Protocol):
    def createDataFrame(
        self,
        data: Iterable[Any],
        schema: StructType
    ) -> DataFrame: ...


class AIProvider(Protocol):
    async def tag_chunk(
        self,
        chunk_id: str,
        source_url: str,
        content: str,
    ) -> Result[BronzeTagResponse, Exception]: ...

    async def embed_text(
        self,
        text: str,
    ) -> Result[list[float], Exception]: ...


class StorageProvider(Protocol):
    def write_records(
        self,
        records: list[BronzeRecord],
    ) -> Result[int, Exception]: ...


class KafkaProvider(Protocol):
    async def send(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
    ) -> Result[bool, Exception]: ...

    async def getmany(
        self,
        *partitions: Any,
        timeout_ms: int | float = 0,
        max_records: int | None = None
    ) -> dict[TopicPartition, list[ConsumerRecord]]: ...


@runtime_checkable
class CrawlerResult(Protocol):
    success: bool
    error_message: str | None
    markdown: str
    metadata: dict[str, Any]


@runtime_checkable
class ChunkingStrategy(Protocol):
    window_size: int
    overlap: int

    def chunk(self, content: str) -> list[str]: ...


@runtime_checkable
class CrawlerRunConfig(Protocol):
    cache_mode: Any
    chunking_strategy: ChunkingStrategy
    markdown_generator: Any


@runtime_checkable
class Crawler(Protocol):
    async def arun(
        self, url: str, config: CrawlerRunConfig) -> CrawlerResult: ...

    async def __aenter__(self) -> "Crawler": ...

    async def __aexit__(self, exc_type: Any, exc_val: Any,
                        exc_tb: Any) -> None: ...


class ScraperProvider(Protocol):
    def scrape_and_chunk(
        self,
        url: str,
        strategy: ChunkingStrategy,
        run_config: CrawlerRunConfig,
    ) -> AsyncIterator[Result[str, Exception]]: ...
