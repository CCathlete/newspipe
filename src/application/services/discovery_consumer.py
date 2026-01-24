# src/application/services/discovery_consumer.py

from dataclasses import dataclass
import json
from typing import Any

from aiokafka.structs import ConsumerRecord
from structlog.typing import FilteringBoundLogger
from returns.result import Result, Success, Failure, safe

from ..models import RelevancePolicy
from ..interfaces import KafkaProvider, AIProvider
from application.services.data_ingestion import IngestionPipeline


# src/domain/services/discovery_consumer.py

@dataclass(slots=True, frozen=True)
class DiscoveryConsumer:
    kafka_provider: KafkaProvider
    llm: AIProvider
    ingestion_pipeline: IngestionPipeline
    logger: FilteringBoundLogger
    discovery_policy: RelevancePolicy

    async def run(self, topics: list[str] = ["raw_chunks"]) -> None:
        self.logger.info("discovery_consumer_started", topics=topics)
        self.kafka_provider.subscribe(topics)

        while True:
            messages = await self.kafka_provider.getmany(timeout_ms=1000, max_records=50)
            if not messages:
                continue

            for records in messages.values():
                for record in records:
                    await self._process_chunk_record(record)

    async def _process_chunk_record(self, record: ConsumerRecord) -> None:
        # Pipeline: Decode -> AI Tagging -> Ingest
        result = (
            Result.from_value(record.value)
            .bind(self._safe_decode)
            .bind(lambda data: Success(data) if "chunk" in data else Failure(ValueError("No chunk")))
        )

        match result:
            case Success(data):
                # Call LiteLLM for tagging
                tag_res = await self.llm.is_relevant(
                    text=data["chunk"], 
                    policy_description=self.discovery_policy.description,
                    language=data.get("language", "en")
                )
                
                match tag_res:
                    case Success(True):
                        # Pass only relevant chunks to ingestion
                        await self.ingestion_pipeline.ingest_relevant_chunk(data)
                        self.logger.info("chunk_ingested", url=data["url"])
                    case Success(False):
                        self.logger.info("chunk_skipped_irrelevant", url=data["url"])
                    case Failure(e):
                        self.logger.info("tagging_failed", error=str(e))
            
            case Failure(e):
                self.logger.info("decode_failed", error=str(e))

    @safe
    def _safe_decode(self, value: bytes | None) -> dict[str, Any]:
        if value is None:
            raise ValueError("Null record value")
        return json.loads(value.decode("utf-8"))
