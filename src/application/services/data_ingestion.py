# src/application/services/data_ingestion.py

from dataclasses import dataclass
from datetime import datetime, UTC
from typing import TypeVar, AsyncIterable, AsyncIterator
from returns.future import FutureResult
from returns.io import IOFailure, IOResult, IOSuccess
from structlog.typing import FilteringBoundLogger
from returns.result import Result, Success, Failure

from domain.models import BronzeRecord, RelevancePolicy
from domain.interfaces import (
    ScraperProvider,
    StorageProvider,
    AIProvider,
    KafkaProvider,
)
from domain.services.linguistic_model import LinguisticService
from domain.interfaces import ChunkingStrategy, CrawlerRunConfig

T = TypeVar("T")

async def _enumerate_async(
    source: AsyncIterable[T], start: int = 0
) -> AsyncIterator[tuple[int, T]]:
    """Asynchronously enumerates items from an AsyncIterable."""
    i = start
    async for item in source:
        yield i, item
        i += 1


def cheap_pre_filter(chunk: str) -> bool:
    """Drop very small or boilerplate chunks before calling LLM."""
    if len(chunk) < 200:
        return False
    lower = chunk.lower()
    if any(term in lower for term in ["cookie", "privacy policy", "terms of service"]):
        return False
    return True


@dataclass(slots=True, frozen=True)
class IngestionPipeline:
    scraper: ScraperProvider
    llm: AIProvider
    lakehouse: StorageProvider
    kafka_producer: KafkaProvider
    logger: FilteringBoundLogger
    strategy: ChunkingStrategy
    run_config: CrawlerRunConfig
    linguistic_service: LinguisticService | None = None
    buffer_size: int = 10

    async def _flush_records_buffer(
        self,
        buffer: list[BronzeRecord],
        current_log: FilteringBoundLogger
    ) -> Result[int, Exception]:

        if not buffer:
            return Success(0)

        future_write_res: FutureResult[int, Exception] = await self.lakehouse.write_records(buffer)
        write_res: IOResult[int, Exception] = await future_write_res.awaitable()
        match write_res:
            case IOSuccess(Success(_)):
                count = len(buffer)
                buffer.clear()
                current_log.info("buffer_flushed", records_written=count)
                return Success(count)

            case IOFailure(Failure(e)):
                current_log.error("buffer_flush_failed", error=str(e))
                return Failure(e)

            case _:
                return Failure(Exception("Unknown issue in ingestion."))

    async def execute(
        self,
        start_url: str,
        language: str,
        policy: RelevancePolicy,
    ) -> Result[int, Exception]:
        """Crawl start_url, filter by policy, and embed relevant chunks."""
        log = self.logger.bind(url=start_url)
        processed_urls: set[str] = set()
        url_queue: list[str] = [start_url]
        buffered_records: list[BronzeRecord] = []
        total_records_ingested: int = 0
        session_ts: float = datetime.now(UTC).timestamp()

        async def _check_and_flush_buffer() -> Result[None, Exception]:
            nonlocal total_records_ingested
            if len(buffered_records) >= self.buffer_size:
                flush_res: Result[int, Exception] = await self._flush_records_buffer(buffered_records, log)
                match flush_res:
                    case Success(count):
                        total_records_ingested += count
                        return Success(None)
                    case Failure(e):
                        return Failure(e)
            return Success(None)

        while url_queue:
            current_url = url_queue.pop(0)
            if current_url in processed_urls:
                continue
            processed_urls.add(current_url)

            async for index, chunk_res in _enumerate_async(
                self.scraper.scrape_and_chunk(
                    url=current_url,
                    strategy=self.strategy,
                    run_config=self.run_config,
                )
            ):
                match chunk_res:
                    case Success(chunk):
                        if not cheap_pre_filter(chunk):
                            continue  # skip irrelevant chunk early

                        # -------------------------
                        # Policy-driven LLM gate
                        # -------------------------
                        gate_res: Result[bool, Exception] = await self.llm.is_relevant(
                            text=chunk,
                            language=language,
                            policy_description=policy.model_dump_json(),
                        )
                        match gate_res:
                            case Success(True):
                                record = BronzeRecord(
                                    chunk_id=f"{current_url}_{index}",
                                    source_url=current_url,
                                    content=chunk,
                                    language=language,
                                    ingested_at=session_ts,
                                )
                                buffered_records.append(record)
                                flush_res = await _check_and_flush_buffer()
                                if isinstance(flush_res, Failure):
                                    return flush_res
                            case Success(False):
                                continue
                            case Failure(e):
                                log.warning(
                                    "gate_failed", url=current_url, error=str(e))
                    case Failure(e):
                        log.warning("chunk_failed", url=current_url, error=str(e))

            # -------------------------
            # Kafka push for new URLs handled separately by crawler
            # -------------------------

        # Final flush
        if buffered_records:
            final_res = await self._flush_records_buffer(buffered_records, log)
            match final_res:
                case Success(count):
                    total_records_ingested += count
                case Failure(e):
                    return Failure(e)

        log.info("pipeline_complete", total_records_ingested=total_records_ingested)
        return Success(total_records_ingested)
