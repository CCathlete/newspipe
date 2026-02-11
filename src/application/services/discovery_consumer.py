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
    processed_count: int = 0
    discovered_count: int = 0



    @future_safe
    async def run(self, seeds: dict[str, list[str]]) -> None:
        # We need to listen to BOTH topics or run two instances
        # Topic 'raw_chunks' -> Ingestion
        # Topic 'discovery_queue' -> Scraping
        topics: list[str] = ["raw_chunks", "discovery_queue", "visited_urls"]
        idle_polls: int = 0
        IDLE_THRESHOLD: int = 10

        
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
                    # ---- IDLE DETECTION ----
                    if not any(messages.values()):
                        idle_polls += 1

                        self.logger.debug(
                            "consumer_idle",
                            idle_polls=idle_polls,
                            visited=len(self.visited),
                            discovered=self.discovered_count,
                            processed=self.processed_count,
                        )

                        if idle_polls >= IDLE_THRESHOLD:
                            self.logger.info(
                                "crawl_quiescent",
                                visited=len(self.visited),
                                discovered=self.discovered_count,
                                processed=self.processed_count,
                            )
                            break  # If we have many idle records we stop the discovery loop.

                        continue

                    # We received messages → wereset idle counter.
                    idle_polls = 0
                    offsets_to_commit: dict[TopicPartition, int] = {}

                    for tp, records in messages.items():
                        for record in records:
                            # Route based on topic
                            if tp.topic == "discovery_queue":
                                discovery_io_result: IOResultE[None] = await self._handle_discovery(record).awaitable()
                                match discovery_io_result:
                                    case IOSuccess(Success(_)):
                                        assert record.value
                                        self.logger.info("Record %s passed to discovery queue.", record.value[:100])
                                    case IOFailure(Failure(e)):
                                        assert record.value
                                        self.logger.error("Record %s failed to pass to discovery queue.", record.value[:100])

                            elif tp.topic == "raw_chunks":
                                ingestion_io_result: IOResultE[None] = await self._handle_ingestion(record).awaitable()
                                match ingestion_io_result:
                                    case IOSuccess(Success(_)):
                                        self.logger.info("Record %s passed to ingestion queue.", record.value)
                                    case IOFailure(Failure(e)):
                                        self.logger.error("Record %s failed to pass to ingestion queue.", record.value)


                            offsets_to_commit[tp] = record.offset


                    if any(messages.values()):
                        await self.kafka_consumer.commit(offsets=offsets_to_commit).awaitable()

                case IOFailure(Failure(e)):
                    self.logger.error("Problem with consumption: %s", e)

    @future_safe
    async def _handle_discovery(self, record: ConsumerRecord) -> None:
        """New URLs found: Trigger the Scraper."""
        decode_result: ResultE[dict[str, Any]] = self._safe_decode(record.value)
        match decode_result:
            case Success(data):
                url: str = data["url"]

                if url in self.visited:
                    self.logger.debug("url_already_visited", url=url)
                    return None
                else:
                    self.discovered_count += 1

                # marking as visited BEFORE crawling.
                self.visited.add(url)

                io_sent_to_queue: IOResultE = await self.visited_producer.send(
                    topic="visited_urls",
                    key=url.encode("utf-8"),
                    value=b"1",
                ).awaitable()

                match io_sent_to_queue:
                    case IOFailure(Failure(err)):
                        self.logger.error("discovery_decode_error", error=str(err))
                        raise

                # We assume standard keys: url, language, strategy_params, etc.
                crawl_future: FutureResultE[None] = self.scraper.deep_crawl(
                    url=data["url"],
                    language=data.get("language", "en")
                )
                res_io: IOResultE[None] = await crawl_future.awaitable()
                
                match res_io:
                    case IOSuccess(Success(_)):
                        self.processed_count += 1
                        self.logger.info("discovery_processed", url=data["url"])
                    case IOFailure(Failure(e)):
                        self.logger.error("discovery_failed", url=data["url"], error=str(e))

            case Failure(err):
                self.logger.error("discovery_decode_error", error=str(err))
                raise

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
