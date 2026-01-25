# src/application/services/discovery_consumer.py

import json
from dataclasses import dataclass
from typing import Any

from aiokafka.structs import ConsumerRecord
from returns.future import FutureResultE, future_safe
from returns.io import IOFailure, IOResultE, IOSuccess
from returns.result import Failure, ResultE, Success, safe
from structlog.typing import FilteringBoundLogger

from domain.interfaces import AIProvider, KafkaProvider
from domain.models import RelevancePolicy
from domain.services.data_ingestion import IngestionPipeline
from domain.services.scraper_service import ScraperService

@dataclass(slots=True, frozen=True)
class DiscoveryConsumer:
    scraper: ScraperService
    ingestion_pipeline: IngestionPipeline
    kafka_connector: KafkaProvider # Used only for the low-level loop
    llm: AIProvider
    logger: FilteringBoundLogger
    discovery_policy: RelevancePolicy

    @future_safe
    async def run(self, seeds: dict[str, list[str]]) -> int:
        topics: list[str] = ["raw_chunks"]
        
        # 1. Orchestrate Domain Scraper: Ensure infra is ready and seeds are pushed
        scraper_future: FutureResultE[list[str]] = self.scraper.initialize_and_seed(seeds, topics)
        scraper_io_res: IOResultE[list[str]] = await scraper_future.awaitable()
        
        match scraper_io_res:
            case IOSuccess(Success(_)):
                self.logger.info("scraper_initialized_and_seeded", topics=topics)
            case IOFailure(Failure(e)):
                self.logger.error("scraper_initialization_failed", error=str(e))
                raise e
            case _:
                raise RuntimeError("Inconsistent monadic state during scraper init")

        # 2. Start Consumption Loop
        self.kafka_connector.subscribe(topics)
        processed_count: int = 0
        
        while True:
            messages = await self.kafka_connector.getmany(timeout_ms=1000, max_records=50)
            if not messages:
                continue

            for records in messages.values():
                for record in records:
                    # We await the future_safe process method
                    await (await self._process_chunk_record(record)).awaitable()
                    processed_count += 1
        
        return processed_count

    @future_safe
    async def _process_chunk_record(self, record: ConsumerRecord) -> None:
        decode_result: ResultE[dict[str, Any]] = self._safe_decode(record.value)

        match decode_result:
            case Success(data):
                if "chunk" not in data:
                    return

                # Request relevance tagging from LLM
                tag_future: FutureResultE[bool] = self.llm.is_relevant(
                    text=data["chunk"],
                    policy_description=self.discovery_policy.description,
                    language=data.get("language", "en")
                )

                io_tag_res: IOResultE[bool] = await tag_future.awaitable()
                
                match io_tag_res:
                    case IOSuccess(Success(is_relevant)):
                        if is_relevant:
                            # Pass to Domain Ingestion Pipeline
                            ingest_future: FutureResultE[None] = self.ingestion_pipeline.ingest_relevant_chunk(data)
                            io_ingest_res = await ingest_future.awaitable()
                        
                            match io_ingest_res:
                                case IOSuccess(Success(count)):
                                    self.logger.info("chunk_ingested", url=data["url"], count=count)
                                case IOFailure(Failure(e)):
                                    self.logger.error("ingestion_io_failed", error=str(e))
                                case _:
                                    self.logger.error("inconsistent_ingestion_state")
                        else:
                            self.logger.debug("chunk_skipped_irrelevant", url=data.get("url"))

                    case IOFailure(Failure(e)):
                        self.logger.error("tagging_io_failed", error=str(e))
                    case _:
                        self.logger.error("inconsistent_tagging_state")

            case Failure(e):
                self.logger.error("decode_failed", error=str(e))

    @safe
    def _safe_decode(self, value: bytes | None) -> dict[str, Any]:
        if value is None:
            raise ValueError("Null record value")
        return json.loads(value.decode("utf-8"))
