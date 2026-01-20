# src/domain/services/discovery_consumer.py
from dataclasses import dataclass, field
import json
import asyncio
from typing import Mapping

from aiokafka.structs import TopicPartition, ConsumerRecord
from structlog.typing import FilteringBoundLogger
from returns.result import Result, Success, Failure

from ..interfaces import KafkaProvider, CrawlerRunConfig
from application.services.data_ingestion import IngestionPipeline


def _resolve_language(
    url: str,
    default_lang: str,
    lookup: Mapping[str, str],
) -> str:
    """Map domain to language if available, otherwise return default."""
    from urllib.parse import urlparse
    domain = urlparse(url).netloc.lower()
    return lookup.get(domain, default_lang)


@dataclass(slots=True, frozen=True)
class DiscoveryConsumer:
    kafka_provider: KafkaProvider
    ingestion_pipeline: IngestionPipeline
    logger: FilteringBoundLogger
    run_config: CrawlerRunConfig
    language_lookup: Mapping[str, str] = field(default_factory=dict)

    async def run(self) -> None:
        self.logger.info("discovery_consumer_started")
        self.kafka_provider.subscribe(["discovery_queue"])

        while True:
            # 1. Consume messages
            messages: dict[TopicPartition, list[ConsumerRecord]] = await self.kafka_provider.getmany(
                timeout_ms=1000, max_records=100
            )
            if not messages:
                await asyncio.sleep(0.1)
                continue

            for partition, records in messages.items():
                for record in records:
                    # 2. Decode payload
                    try:
                        assert isinstance(record.value, bytes)
                        payload = json.loads(record.value.decode("utf-8"))
                    except Exception as exc:
                        self.logger.warning(
                            "json_decode_failed", error=str(exc))
                        continue

                    url = payload.get("url")
                    if not url:
                        self.logger.warning(
                            "malformed_message", payload=payload)
                        continue

                    # 3. Determine language
                    payload_lang = payload.get("language")
                    parent_default = getattr(self.run_config, "language", "en")
                    language = (
                        payload_lang
                        or _resolve_language(url, parent_default, self.language_lookup)
                    )

                    # 4. Delegate to pipeline
                    result: Result[int, Exception] = await self.ingestion_pipeline.execute(
                        start_url=url,
                        language=language,
                    )

                    match result:
                        case Success(cnt):
                            self.logger.info(
                                "crawled_child", url=url, records_written=cnt)
                        case Failure(e):
                            self.logger.error(
                                "crawling_failed", url=url, error=str(e))
