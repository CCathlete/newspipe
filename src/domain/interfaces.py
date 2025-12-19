# src/domain/interfaces.py

from typing import Protocol, AsyncIterator
from returns.result import Result
from .models import BronzeTagResponse, BronzeRecord


class ScraperProvider(Protocol):
    async def scrape_and_chunk(
        self,
        url: str,
    ) -> AsyncIterator[Result[str, Exception]]: ...


class AIProvider(Protocol):
    async def tag_chunk(
        self,
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
    async def produce(
        self, topic: str,
        value: bytes,
        key: bytes | None = None,
    ) -> Result[bool, Exception]: ...
