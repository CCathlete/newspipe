# src/application/services/discovery_consumer.py

import json
from dataclasses import dataclass, field
from typing import Any

from aiokafka.structs import ConsumerRecord, TopicPartition
from returns.future import FutureResultE, future_safe
from returns.io import IOFailure, IOResultE, IOSuccess
from returns.result import Failure, ResultE, Success, safe
from structlog.typing import FilteringBoundLogger

from domain.interfaces import KafkaPort, ScraperPort
from domain.services.data_ingestion import IngestionPipeline

@dataclass(slots=True, frozen=False)
class DiscoveryConsumer:
    scraper: ScraperPort
    ingestion_pipeline: IngestionPipeline
    kafka_consumer: KafkaPort
    visited_producer: KafkaPort
    logger: FilteringBoundLogger

    visited: set[str] = field(default_factory=set)


    @future_safe
    async def run(self, seeds: dict[str, list[str]]) -> None:
        # We need to listen to BOTH topics or run two instances
        # Topic 'raw_chunks' -> Ingestion
        # Topic 'discovery_queue' -> Scraping
        topics: list[str] = ["raw_chunks", "discovery_queue"]
        
        # 1. Initialize infra and seeds
        scraper_future: FutureResultE[list[str]] = self.scraper.initialize_and_seed(seeds, topics)
        scraper_io: IOResultE[list[str]] = await scraper_future.awaitable()
        
        match scraper_io:
            case IOSuccess(Success(_)):
                self.logger.info("consumer_loop_starting", topics=topics)
            case IOFailure(Failure(e)):
                self.logger.error("initialization_critical_failure", error=str(e))
                raise e
            case _:
                raise RuntimeError("Inconsistent state")

        self.kafka_consumer.subscribe(topics)
        
        while True:
            messages_future: FutureResultE[dict[TopicPartition, list[ConsumerRecord[Any, Any]]]] = self.kafka_consumer.getmany(
                timeout_ms=1000,
                max_records=50
            )
            messages_io_monad: IOResultE[dict[TopicPartition, list[ConsumerRecord[Any, Any]]]] = await messages_future.awaitable()

            match messages_io_monad:
                case IOSuccess(Success(messages)):
                    for tp, records in messages.items():
                        for record in records:
                            # Route based on topic
                            if tp.topic == "discovery_queue":
                                discovery_io_result: IOResultE[None] = await self._handle_discovery(record).awaitable()
                                match discovery_io_result:
                                    case IOSuccess(Success(_)):
                                        self.logger.info("Record %s passed to discovery queue.", record.value)
                                    case IOFailure(Failure(e)):
                                        self.logger.error("Record %s failed to pass to discovery queue.", record.value)

                            elif tp.topic == "raw_chunks":
                                ingestion_io_result: IOResultE[None] = await self._handle_ingestion(record).awaitable()
                                match ingestion_io_result:
                                    case IOSuccess(Success(_)):
                                        self.logger.info("Record %s passed to ingestion queue.", record.value)
                                    case IOFailure(Failure(e)):
                                        self.logger.error("Record %s failed to pass to ingestion queue.", record.value)

                case IOFailure(Failure(e)):
                    self.logger.error("Problem with consumption: %s", e)

    @future_safe
    async def _handle_discovery(self, record: ConsumerRecord) -> None:
        """New URLs found: Trigger the Scraper."""
        decode_result: ResultE[dict[str, Any]] = self._safe_decode(record.value)
        match decode_result:
            case Success(data):
                # We assume standard keys: url, language, strategy_params, etc.
                crawl_future: FutureResultE[None] = self.scraper.deep_crawl(
                    url=data["url"],
                    language=data.get("language", "en")
                )
                res_io: IOResultE[None] = await crawl_future.awaitable()
                
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
            case Success(data_bronzerecord):
                # The IngestionPipeline handles its own LLM tagging.
                ingest_f: FutureResultE[None] = self.ingestion_pipeline.ingest_if_relevant(data_bronzerecord)
                res_io: IOResultE[None] = await ingest_f.awaitable()
                
                match res_io:
                    case IOSuccess(Success(_)):
                        self.logger.info("chunk_persisted", url=data_bronzerecord["source_url"])
                    case IOFailure(Failure(e)):
                        self.logger.error("ingestion_failed", url=data_bronzerecord["source_url"], error=str(e))
            case Failure(e):
                self.logger.error("ingestion_decode_error", error=str(e))

    @safe
    def _safe_decode(self, value: bytes | None) -> dict[str, Any]:
        if value is None: raise ValueError("Empty payload")
        return json.loads(value.decode("utf-8"))
