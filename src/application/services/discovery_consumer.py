# src/application/services/discovery_consumer.py

import json
from dataclasses import dataclass
from typing import Any

from aiokafka.structs import ConsumerRecord
from returns.future import future_safe
from returns.io import IOFailure, IOSuccess
from returns.result import Failure, Result, Success, safe
from structlog.typing import FilteringBoundLogger

from domain.interfaces import AIProvider, KafkaProvider
from domain.models import RelevancePolicy
from domain.services.data_ingestion import IngestionPipeline

@dataclass(slots=True, frozen=True)
class DiscoveryConsumer:
    kafka_provider: KafkaProvider
    llm: AIProvider
    ingestion_pipeline: IngestionPipeline
    logger: FilteringBoundLogger
    discovery_policy: RelevancePolicy

    @future_safe
    async def run(self, seeds: dict[str, list[str]]) -> int:
        topics: list[str] = ["raw_chunks"]
        
        # --- Infrastructure Verification ---
        infra_future = await self.kafka_provider.ensure_topics_exist(topics)
        infra_res = await infra_future.awaitable()
        
        match infra_res:
            case IOSuccess(res):
                match res:
                    case Success(_):
                        self.logger.info("infrastructure_ready", topics=topics)
                    case Failure(e):
                        self.logger.error("topic_creation_logic_failed", error=str(e))
                        raise e
            case IOFailure(e):
                self.logger.error("topic_creation_io_failed", error=str(e))
                raise e

        # --- Seed Injection ---
        await self.kafka_provider.produce_seeds(seeds)
        
        self.logger.info("discovery_consumer_started", topics=topics)
        self.kafka_provider.subscribe(topics)

        processed_count: int = 0
        while True:
            messages = await self.kafka_provider.getmany(timeout_ms=1000, max_records=50)
            if not messages:
                continue

            for records in messages.values():
                for record in records:
                    await self._process_chunk_record(record)
                    processed_count += 1
        
        return processed_count

    async def _process_chunk_record(self, record: ConsumerRecord) -> None:
        decode_result: Result[dict[str, Any], Exception] = (
            Result.from_value(record.value)
            .bind(self._safe_decode)
        )
        
        match decode_result:
            case Success(data):
                if "chunk" not in data:
                    return

                tag_future: Result[bool, Exception] = await self.llm.is_relevant(
                    text=data["chunk"],
                    policy_description=self.discovery_policy.description,
                    language=data.get("language", "en")
                )
                io_tag_res = await tag_future.awaitable()
                
                match io_tag_res:
                    case IOSuccess(res):
                        match res:
                            case Success(True):
                                ingest_future = await self.ingestion_pipeline.ingest_relevant_chunk(data)
                                io_ingest_res = await ingest_future.awaitable()
                                
                                match io_ingest_res:
                                    case IOSuccess(i_res):
                                        match i_res:
                                            case Success(count):
                                                self.logger.info("chunk_ingested", url=data["url"], count=count)
                                            case Failure(e):
                                                self.logger.error("ingestion_logic_failed", error=str(e))
                                    case IOFailure(e):
                                        self.logger.error("ingestion_io_failed", error=str(e))
                            case Success(False):
                                pass
                            case Failure(e):
                                self.logger.error("tagging_logic_failed", error=str(e))
                    case IOFailure(e):
                        self.logger.error("tagging_io_failed", error=str(e))
            case Failure(e):
                self.logger.error("decode_failed", error=str(e))

    @safe
    def _safe_decode(self, value: bytes | None) -> dict[str, Any]:
        if value is None:
            raise ValueError("Null record value")
        return json.loads(value.decode("utf-8"))
