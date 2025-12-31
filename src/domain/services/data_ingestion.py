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

    async def execute(
        self,
        start_url: str,
        language: str,
    ) -> Result[int, Exception]:
        """
        Crawl ``start_url`` and everything discovered via CLICKLINK,
        persisting all ``BronzeRecord`` objects to the lakehouse.
        Returns the total number of records written.
        """
        log = self.logger.bind(url=start_url)

        processed: Set[str] = set()
        queue: List[str] = [start_url]
        all_records: List[BronzeRecord] = []
        session_ts = datetime.now(UTC).timestamp()

        while queue:
            url = queue.pop(0)
            if url in processed:
                continue
            processed.add(url)

            async for index, outcome in _enumerate_async(
                self.scraper.scrape_and_chunk(
                    url=url,
                    strategy=self.strategy,
                    run_config=self.run_config,
                )
            ):
                match outcome:
                    case Success(chunk):
                        chunk_id = f"{url}_{index}"
                        tag_res = await self.ollama.tag_chunk(
                            chunk_id=chunk_id,
                            source_url=url,
                            content=chunk,
                        )
                        match tag_res:
                            case Success(tag):
                                # ── base record ───────────────────────────────────────
                                base = BronzeRecord(
                                    chunk_id=tag.chunk_id,
                                    source_url=url,
                                    content=chunk,
                                    control_action=tag.control_action,
                                    language=language,
                                    ingested_at=session_ts,
                                )
                                all_records.append(base)

                                # ── optional semantic grams ───────────────────────
                                if self.linguistic_service:
                                    match await self.linguistic_service.generate_semantic_records(
                                        text=chunk,
                                        language=language,
                                        base_record=base,
                                    ):
                                        case Success(grams):
                                            all_records.extend(grams)
                                        case Failure(e):
                                            log.warning(
                                                "gram_generation_failed",
                                                error=str(e),
                                            )
                                        case _:
                                            pass

                                # ── discover new CLICKLINK URLs ─────────────────
                                discovered: Set[str] = set()
                                if tag.control_action == "CLICKLINK" and tag.source_url:
                                    discovered.add(tag.source_url)
                                if tag.actions:
                                    for a in tag.actions:
                                        if a.get("control_action") == "CLICKLINK":
                                            child = a.get("url")
                                            if child:
                                                discovered.add(child)

                                for child_url in discovered:
                                    await self.kafka_producer.send(
                                        topic="discovery_queue",
                                        value=json.dumps(
                                            {"url": child_url}).encode(),
                                    )
                                    if child_url not in processed:
                                        queue.append(child_url)

                            case Failure(e):
                                log.warning("tagging_failed", error=str(e))
                            case _:
                                pass
                    case Failure(e):
                        log.error("stream_failed", error=str(e))
                        continue
                    case _:
                        pass

        # Persist everything that was discovered in this *crawl tree*
        write_res = self.lakehouse.write_records(all_records)
        log.info("pipeline_stage_complete", records_written=len(all_records))
        return Success(len(all_records))
