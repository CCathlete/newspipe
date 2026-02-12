# src/application/services/ingestion_service.py

import json
import asyncio
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
class IngestionService:
    ingestion_pipeline: IngestionPipeline
    kafka_consumer: KafkaPort
    logger: FilteringBoundLogger

    visited: set[str] = field(default_factory=set)
    active_tasks: set[asyncio.Task] = field(default_factory=set)

    processed_count: int = 0
    discovered_count: int = 0
    # in_flight: int = 0


    def _on_task_done(self, task: asyncio.Task) -> None:
        self.active_tasks.discard(task)

    @future_safe
    async def run(self) -> None:
        topics: list[str] = ["raw_chunks"]
        idle_polls: int = 0
        IDLE_THRESHOLD: int = 10

        self.kafka_consumer.subscribe(topics)
        
        while True:
            messages_future: FutureResultE[dict[TopicPartition, list[ConsumerRecord[Any, Any]]]] = self.kafka_consumer.getmany(
                timeout_ms=1000,
                # max_records=50,
                max_records=5,
            )
            messages_io_monad: IOResultE[dict[TopicPartition, list[ConsumerRecord[Any, Any]]]] = await messages_future.awaitable()

            match messages_io_monad:
                case IOSuccess(Success(messages)):
                    # ---- IDLE DETECTION ----
                    if not any(messages.values()) and len(self.active_tasks) == 0:
                        idle_polls += 1

                        self.logger.info(
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

                            self.logger.info("waiting_for_inflight_tasks", count=len(self.active_tasks))
                            await asyncio.gather(*self.active_tasks, return_exceptions=True)
                            break  # If we have many idle records we stop the discovery loop.

                        continue

                    # We received messages → we reset idle counter.
                    idle_polls = 0
                    offsets_to_commit: dict[TopicPartition, int] = {}

                    for tp, records in messages.items():
                        for record in records:
                            if tp.topic == "raw_chunks":
                                ingestion_io_result: IOResultE[None] = self._handle_ingestion(record)
                                match ingestion_io_result:
                                    case IOSuccess(Success(_)):
                                        assert record.value
                                        self.logger.info("Record %s passed to ingestion queue.", record.value[:100])
                                    case IOFailure(Failure(e)):
                                        assert record.value
                                        self.logger.error("Record %s failed to pass to ingestion queue.", record.value[:100])


                            offsets_to_commit[tp] = record.offset


                    if offsets_to_commit:
                        await self.kafka_consumer.commit(offsets=offsets_to_commit).awaitable()

                case IOFailure(Failure(e)):
                    self.logger.error("Problem with consumption: %s", e)

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
