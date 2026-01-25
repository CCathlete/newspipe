# src/application/services/discovery_consumer.py

import json
from dataclasses import dataclass
from typing import Any

from aiokafka.structs import ConsumerRecord
from returns.future import FutureResultE, future_safe
from returns.io import IOFailure, IOResultE, IOSuccess
from returns.result import Failure, ResultE, Success, safe
from structlog.typing import FilteringBoundLogger

from domain.interfaces import KafkaProvider, ScraperProvider
from domain.services.data_ingestion import IngestionPipeline

@dataclass(slots=True, frozen=True)
class DiscoveryConsumer:
    scraper: ScraperProvider
    ingestion_pipeline: IngestionPipeline
    kafka_connector: KafkaProvider
    logger: FilteringBoundLogger

    @future_safe
    async def run(self, seeds: dict[str, list[str]]) -> None:
        # We need to listen to BOTH topics or run two instances
        # Topic 'raw_chunks' -> Ingestion
        # Topic 'discovery_queue' -> Scraping
        topics: list[str] = ["raw_chunks", "discovery_queue"]
        
        # 1. Initialize infra and seeds
        scraper_f: FutureResultE[list[str]] = self.scraper.initialize_and_seed(seeds, topics)
        scraper_io: IOResultE[list[str]] = await scraper_f.awaitable()
        
        match scraper_io:
            case IOSuccess(Success(_)):
                self.logger.info("consumer_loop_starting", topics=topics)
            case IOFailure(Failure(e)):
                self.logger.error("initialization_critical_failure", error=str(e))
                raise e
            case _:
                raise RuntimeError("Inconsistent state")

        self.kafka_connector.subscribe(topics)
        
        while True:
            messages = await self.kafka_connector.getmany(timeout_ms=1000, max_records=50)
            if not messages:
                continue

            for tp, records in messages.items():
                for record in records:
                    # Route based on topic
                    if tp.topic == "discovery_queue":
                        await (await self._handle_discovery(record)).awaitable()
                    elif tp.topic == "raw_chunks":
                        await (await self._handle_ingestion(record)).awaitable()

    @future_safe
    async def _handle_discovery(self, record: ConsumerRecord) -> None:
        """New URLs found: Trigger the Scraper."""
        decode_result: ResultE[dict[str, Any]] = self._safe_decode(record.value)
        match decode_result:
            case Success(data):
                # We assume standard keys: url, language, strategy_params, etc.
                crawl_f: FutureResultE[bool] = self.scraper.deep_crawl(
                    url=data["url"],
                    strategy=..., # Pass from message or use default
                    run_config=...,
                    language=data.get("language", "en")
                )
                res_io: IOResultE[bool] = await crawl_f.awaitable()
                
                match res_io:
                    case IOSuccess(Success(_)):
                        self.logger.info("discovery_processed", url=data["url"])
                    case IOFailure(Failure(e)):
                        self.logger.error("discovery_failed", url=data["url"], error=str(e))
            case Failure(e):
                self.logger.error("discovery_decode_error", error=str(e))

    @future_safe
    async def _handle_ingestion(self, record: ConsumerRecord) -> None:
        """Markdown chunks found: Trigger LLM Tagging & Storage."""
        match self._safe_decode(record.value):
            case Success(data):
                # The IngestionPipeline handles its own LLM tagging monadically
                ingest_f: FutureResultE[None] = self.ingestion_pipeline.ingest_relevant_chunk(data)
                res_io: IOResultE[None] = await ingest_f.awaitable()
                
                match res_io:
                    case IOSuccess(Success(_)):
                        self.logger.info("chunk_persisted", url=data["url"])
                    case IOFailure(Failure(e)):
                        self.logger.error("ingestion_failed", url=data["url"], error=str(e))
            case Failure(e):
                self.logger.error("ingestion_decode_error", error=str(e))

    @safe
    def _safe_decode(self, value: bytes | None) -> dict[str, Any]:
        if value is None: raise ValueError("Empty payload")
        return json.loads(value.decode("utf-8"))
