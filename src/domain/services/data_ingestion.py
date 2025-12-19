# src/domain/services/data_ingestion.py

import json
from dataclasses import dataclass, field
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


@dataclass(slots=True, frozen=True)
class IngestionPipeline:
    scraper: ScraperProvider
    ollama: AIProvider
    lakehouse: StorageProvider
    kafka_producer: KafkaProvider
    linguistic_service: LinguisticService | None = None

    logger: FilteringBoundLogger = field(init=False)

    async def execute(self, url: str) -> Result[int, Exception]:
        log = self.logger.bind(url=url)
        records: list[BronzeRecord] = []

        async for chunk_result in await self.scraper.scrape_and_chunk(url):
            match chunk_result:
                case Success(chunk):
                    tag_result = await self.ollama.tag_chunk(
                        source_url=url,
                        content=chunk
                    )

                    match tag_result:
                        case Success(tag):

                            # 1. Create and add the Raw Record
                            base_record = BronzeRecord(
                                chunk_id=tag.chunk_id,
                                source_url=url,
                                content=chunk,
                                control_action=tag.control_action,
                                language=self.linguistic_service.language if self.linguistic_service else "sk"
                            )
                            records.append(base_record)

                            # 2. Toggleable Gram Building
                            if self.linguistic_service:
                                match await self.linguistic_service.generate_semantic_records(chunk, base_record):
                                    case Success(grams):
                                        records.extend(grams)
                                    case _: pass

                            # 3. Kafka side-effects
                            if tag.control_action == "CLICKLINK":
                                match tag.metadata.map(lambda m: m.get("url")).bind_optional(lambda x: x):
                                    case Some(url_val):
                                        await self.kafka_producer.produce(
                                            topic="discovery_queue",
                                            value=json.dumps(
                                                {"url": url_val}).encode()
                                        )
                                    case _: pass

                        case Failure(e):
                            log.warning("tagging_failed", error=str(e))

                        case _: pass

                case Failure(e):
                    log.error("stream_failed", error=str(e))
                    continue

                case _: pass

        return self.lakehouse.write_records(records)
