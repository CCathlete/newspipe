# src/domain/services/data_ingestion.py

import json
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import AsyncIterable, List, Set, TypeVar, AsyncIterator
from structlog.typing import FilteringBoundLogger
from returns.result import Result, Success, Failure

from ..models import BronzeRecord
from ..interfaces import (
    ScraperProvider,
    StorageProvider,
    AIProvider,
    KafkaProvider,
)
from .linguistic_model import LinguisticService
from ..interfaces import ChunkingStrategy, CrawlerRunConfig

T = TypeVar("T")


async def _enumerate_async(
    source: AsyncIterable[T], start: int = 0
) -> AsyncIterator[tuple[int, T]]:
    """Asynchronously enumerates items from an AsyncIterable."""
    i = start
    async for item in source:
        yield i, item
        i += 1


@dataclass(slots=True, frozen=True)
class IngestionPipeline:
    scraper: ScraperProvider
    ollama: AIProvider
    lakehouse: StorageProvider
    kafka_producer: KafkaProvider
    logger: FilteringBoundLogger
    strategy: ChunkingStrategy
    run_config: CrawlerRunConfig
    linguistic_service: LinguisticService | None = field(default=None)
    buffer_size: int = field(default=10)

    async def _flush_records_buffer(self, buffer: List[BronzeRecord], current_log: FilteringBoundLogger) -> Result[int, Exception]:
        """
        Flushes the provided buffer of BronzeRecord objects to the lakehouse.
        Clears the buffer upon successful write.
        Returns the number of records written or a Failure if an error occurs.
        """
        if not buffer:
            return Success(0)

        # We assume lakehouse.write_records is an awaitable that returns Result[int, Exception]
        # where int is the count of records successfully written.
        write_result = await self.lakehouse.write_records(buffer)
        match write_result:
            case Success(_):
                # Assuming all records in the buffer were written on Success
                count = len(buffer)
                current_log.info(
                    "buffer_flushed", records_written=count, buffer_size_at_flush=count)
                buffer.clear()  # Crucially clear the buffer after a successful write
                return Success(count)
            case Failure(e):
                current_log.error("buffer_flush_failed", error=str(e))
                return Failure(e)
            case _:
                return Failure(Exception("Unexpected result type from lakehouse.write_records"))

    async def execute(
        self,
        start_url: str,
        language: str,
    ) -> Result[int, Exception]:
        """
        Crawl ``start_url`` and everything discovered via CLICKLINK,
        persisting ``BronzeRecord`` objects to the lakehouse in buffered batches.
        Returns the total number of records written.
        """
        log = self.logger.bind(url=start_url)
        processed_urls: Set[str] = set()
        url_queue: List[str] = [start_url]
        # Changed: Buffer to hold records
        buffered_records: List[BronzeRecord] = []
        total_records_ingested: int = 0  # New: Counter for total records written
        session_ts = datetime.now(UTC).timestamp()

        # Helper function to check buffer size and flush if capacity is met
        async def _check_and_flush_buffer() -> Result[None, Exception]:
            nonlocal total_records_ingested
            if len(buffered_records) >= self.buffer_size:
                flush_res = await self._flush_records_buffer(buffered_records, log)
                match flush_res:
                    case Success(count):
                        total_records_ingested += count
                        return Success(None)
                    case Failure(e):
                        # Error has already been logged by _flush_records_buffer
                        return Failure(e)
            return Success(None)  # No flush needed, or buffer not yet full

        while url_queue:
            current_url = url_queue.pop(0)
            if current_url in processed_urls:
                continue
            processed_urls.add(current_url)

            async for index, outcome in _enumerate_async(
                self.scraper.scrape_and_chunk(
                    url=current_url,
                    strategy=self.strategy,
                    run_config=self.run_config,
                )
            ):
                match outcome:
                    case Success(chunk):
                        chunk_id = f"{current_url}_{index}"
                        tag_res = await self.ollama.tag_chunk(
                            chunk_id=chunk_id,
                            source_url=current_url,
                            content=chunk,
                        )
                        match tag_res:
                            case Success(tag):
                                # ── base record ───────────────────────────────────────
                                base_record = BronzeRecord(
                                    chunk_id=tag.chunk_id,
                                    source_url=current_url,
                                    content=chunk,
                                    control_action=tag.control_action,
                                    language=language,
                                    ingested_at=session_ts,
                                )
                                buffered_records.append(base_record)

                                # Attempt to flush after adding base record
                                flush_attempt_res = await _check_and_flush_buffer()
                                if isinstance(flush_attempt_res, Failure):
                                    return flush_attempt_res  # Propagate failure immediately

                                # ── optional semantic grams ───────────────────────
                                if self.linguistic_service:
                                    generate_grams_res = await self.linguistic_service.generate_semantic_records(
                                        text=chunk,
                                        language=language,
                                        base_record=base_record,
                                    )
                                    match generate_grams_res:
                                        case Success(grams):
                                            buffered_records.extend(grams)
                                            # Attempt to flush after adding semantic grams
                                            flush_attempt_res = await _check_and_flush_buffer()
                                            if isinstance(flush_attempt_res, Failure):
                                                return flush_attempt_res  # Propagate failure immediately
                                        case Failure(e):
                                            log.warning("gram_generation_failed", error=str(
                                                e), chunk_id=chunk_id)
                                        case _:
                                            pass

                                # ── discover new CLICKLINK URLs ─────────────────
                                discovered_urls: Set[str] = set()
                                if tag.control_action == "CLICKLINK" and tag.source_url:
                                    discovered_urls.add(tag.source_url)
                                if tag.actions:
                                    for action in tag.actions:
                                        if action.get("control_action") == "CLICKLINK":
                                            child_url = action.get("url")
                                            # Ensure child_url is a string
                                            if isinstance(child_url, str):
                                                discovered_urls.add(child_url)

                                for child_url in discovered_urls:
                                    await self.kafka_producer.send(
                                        topic="discovery_queue",
                                        value=json.dumps(
                                            {"url": child_url}).encode(),
                                    )
                                    if child_url not in processed_urls:
                                        url_queue.append(child_url)
                            case Failure(e):
                                log.warning("tagging_failed", error=str(
                                    e), url=current_url, chunk_id=chunk_id)
                            case _:
                                pass
                    case Failure(e):
                        log.error("stream_failed", error=str(
                            e), url=current_url)
                        continue
                    case _:
                        pass

        # Final flush: Ensure any remaining records in the buffer are written
        if buffered_records:
            final_flush_res = await self._flush_records_buffer(buffered_records, log)
            match final_flush_res:
                case Success(count):
                    total_records_ingested += count
                case Failure(e):
                    # Error has already been logged by _flush_records_buffer
                    return Failure(e)
                case _:
                    return Failure(Exception("Unexpected final flush result"))

        log.info("pipeline_stage_complete",
                 total_records_written=total_records_ingested)
        return Success(total_records_ingested)
