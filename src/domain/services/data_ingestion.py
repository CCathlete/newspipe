# src/domain/services/data_ingestion.py

import json
from dataclasses import dataclass
from datetime import datetime, UTC
from structlog.typing import FilteringBoundLogger
from returns.result import Success, Failure, Result
from returns.maybe import Some
from ..models import BronzeRecord
from ..interfaces import (
    ScraperProvider,
    StorageProvider,
    AIProvider,
    KafkaProvider,
)
from .linguistic_model import LinguisticService
from ..interfaces import ChunkingStrategy, CrawlerRunConfig

from collections.abc import AsyncIterator
from typing import TypeVar

T = TypeVar("T")


async def enumerate_async(iterable: AsyncIterator[T], start: int = 0) -> AsyncIterator[tuple[int, T]]:
    i = start
    async for item in iterable:
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
    linguistic_service: LinguisticService

    async def execute(
        self,
        url: str,
        language: str,
    ) -> Result[int, Exception]:
        log = self.logger.bind(url=url)
        records: list[BronzeRecord] = []

        # We generate a consistent timestamp for all chunks in this session
        session_timestamp = datetime.now(UTC).timestamp()

        async for index, chunk_result in enumerate_async(
            self.scraper.scrape_and_chunk(
                url=url,
                strategy=self.strategy,
                run_config=self.run_config,
            )
        ):
            match chunk_result:
                case Success(chunk):
                    chunk_id: str = f"{url}_{index}"
                    tag_result = await self.ollama.tag_chunk(
                        chunk_id=chunk_id,
                        source_url=url,
                        content=chunk
                    )

                    match tag_result:
                        case Success(tag):
                            # 1. Create and add the Raw Record
                            # Using the session_timestamp since tag doesn't hold one
                            base_record = BronzeRecord(
                                chunk_id=tag.chunk_id,
                                source_url=url,
                                content=chunk,
                                control_action=tag.control_action,
                                language=language,
                                ingested_at=session_timestamp
                            )
                            records.append(base_record)

                            # 2. Toggleable Gram Building
                            if self.linguistic_service:
                                match await self.linguistic_service.generate_semantic_records(
                                        text=chunk,
                                        language=language,
                                        base_record=base_record,
                                ):

                                    case Success(grams):
                                        records.extend(grams)

                                    case Failure(e):
                                        log.warning(
                                            "gram_generation_failed", error=str(e))

                                    case _: pass

                            # 3. Kafka side-effects for CLICKLINK.
                            if tag.control_action == "CLICKLINK":
                                # Monadic extraction from the 'metadata' Field.
                                match tag.metadata.bind_optional(
                                    lambda m: m.get("source_url")
                                ):

                                    case Some(url_val):
                                        await self.kafka_producer.produce(
                                            topic="discovery_queue",
                                            value=json.dumps(
                                                {"url": url_val}
                                            ).encode()
                                        )

                                    case _:
                                        log.debug(
                                            "clicklink_missing_url", chunk_id=tag.chunk_id)

                            # 4. Kafka side-effects for actions with CLICKLINK.
                            match tag.metadata.bind_optional(
                                lambda m: m.get("actions")
                            ):

                                case Some(actions_list):
                                    for action_dict in actions_list:
                                        if action_dict.get("control_action") == "CLICKLINK":
                                            await self.kafka_producer.produce(
                                                topic="discovery_queue",
                                                value=json.dumps(
                                                    {"url": action_dict.get(
                                                        "url")}
                                                ).encode()
                                            )

                                case _:
                                    log.debug(
                                        "No_actions_found", chunk_id=tag.chunk_id)

                        case Failure(e):
                            log.warning("tagging_failed", error=str(e))

                        case _: pass

                case Failure(e):
                    log.error("stream_failed", error=str(e))
                    continue

                case _: pass

        return self.lakehouse.write_records(records)
