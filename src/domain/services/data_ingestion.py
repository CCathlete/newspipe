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

        async for chunk_result in self.scraper.scrape_and_chunk(
            url=url,
            strategy=self.strategy,
            run_config=self.run_config,
        ):
            match chunk_result:
                case Success(chunk):
                    tag_result = await self.ollama.tag_chunk(
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
                                language=language if self.linguistic_service else "sk",
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

                            # 3. Kafka side-effects for CLICKLINK
                            if tag.control_action == "CLICKLINK":
                                # Monadic extraction from the 'metadata' Field
                                match tag.metadata.bind_optional(lambda m: m.get("url")):

                                    case Some(url_val):
                                        await self.kafka_producer.produce(
                                            topic="discovery_queue",
                                            value=json.dumps(
                                                {"url": url_val}).encode()
                                        )

                                    case _:
                                        log.debug(
                                            "clicklink_missing_url", chunk_id=tag.chunk_id)

                        case Failure(e):
                            log.warning("tagging_failed", error=str(e))

                        case _: pass

                case Failure(e):
                    log.error("stream_failed", error=str(e))
                    continue

                case _: pass

        return self.lakehouse.write_records(records)
