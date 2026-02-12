# src/application/services/ingestion_service.py

import json
import asyncio
from dataclasses import dataclass
from typing import Any

from aiokafka.structs import ConsumerRecord, TopicPartition
from returns.future import FutureResultE, future_safe
from returns.io import IOResultE, IOSuccess, IOFailure
from returns.result import ResultE, Success, Failure, safe
from structlog.typing import FilteringBoundLogger

from domain.interfaces import KafkaPort
from domain.services.data_ingestion import IngestionPipeline


@dataclass(slots=True)
class IngestionService:
    ingestion_pipeline: IngestionPipeline
    kafka_consumer: KafkaPort
    logger: FilteringBoundLogger

    @future_safe
    async def run(self) -> None:
        topics = ["relevant_chunks"]
        self.kafka_consumer.subscribe(topics)
        self.logger.info("ingestion_service_starting", topics=topics)

        while True:
            messages_future: FutureResultE[
                dict[TopicPartition, list[ConsumerRecord[Any, Any]]]
            ] = self.kafka_consumer.getmany(timeout_ms=1000, max_records=50)

            messages_io: IOResultE[
                dict[TopicPartition, list[ConsumerRecord[Any, Any]]]
            ] = await messages_future.awaitable()

            match messages_io:
                case IOSuccess(Success(messages)):
                    if not any(messages.values()):
                        self.logger.debug("no_messages_received")
                        await asyncio.sleep(0.1)
                        continue

                    for tp, records in messages.items():
                        for record in records:
                            await self._handle_ingestion(record, tp)

                case IOFailure(Failure(e)):
                    self.logger.error("kafka_get_failed", error=str(e))
                    await asyncio.sleep(1)

    @future_safe
    async def _handle_ingestion(self, record: ConsumerRecord, tp: TopicPartition) -> None:
        data_result: ResultE[dict[str, Any]] = self._safe_decode(record.value)

        match data_result:
            case Success(data):
                try:
                    # Run Spark ingestion synchronously in a thread
                    await asyncio.to_thread(self.ingestion_pipeline.ingest_if_relevant, data)

                    # Commit after successful write
                    await self.kafka_consumer.commit({tp: record.offset}).awaitable()
                    self.logger.info("chunk_written", url=data["source_url"])

                except Exception as e:
                    self.logger.error(
                        "chunk_write_failed",
                        url=data.get("source_url"),
                        error=str(e),
                    )

            case Failure(err):
                self.logger.error("decode_failed", error=str(err))

    @safe
    def _safe_decode(self, value: bytes | None) -> dict[str, Any]:
        if value is None:
            raise ValueError("Empty payload")
        return json.loads(value.decode("utf-8"))

