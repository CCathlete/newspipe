# src/domain/services/discovery_consumer.py

import json
from dataclasses import dataclass
from typing import Any
from structlog.typing import FilteringBoundLogger
from returns.result import Success, Failure
from ..interfaces import KafkaProvider, CrawlerRunConfig
from .data_ingestion import IngestionPipeline


@dataclass(slots=True, frozen=True)
class DiscoveryConsumer:
    kafka_provider: KafkaProvider
    ingestion_pipeline: IngestionPipeline
    logger: FilteringBoundLogger
    run_config: CrawlerRunConfig

    async def run(self) -> None:
        self.logger.info("discovery_consumer_started")
        self.kafka_provider.subscribe("discovery_queue")

        while True:
            try:
                messages = await self.kafka_provider.getmany(timeout_ms=1000, max_records=100)
                for partition, records in messages.items():
                    for record in records:
                        try:
                            payload = json.loads(record.value)
                            url = payload.get("url")
                            if not url:
                                self.logger.warning(
                                    "malformed_message", msg=record)
                                continue

                            if self.run_config.depth_limit <= 0:
                                self.logger.info(
                                    "depth_limit_reached", url=url)
                                continue

                            child_cfg = self.run_config.replace(
                                depth_limit=self.run_config.depth_limit - 1
                            )
                            child_pipe = self.ingestion_pipeline.replace(
                                run_config=child_cfg)
                            result = await child_pipe.execute(url=url, language=self.run_config.language)

                            match result:
                                case Success(cnt):
                                    self.logger.info(
                                        "crawled_child", url=url, records_written=cnt)
                                case Failure(e):
                                    self.logger.error(
                                        "crawling_failed", url=url, error=str(e))
                                case _:
                                    pass
                        except json.JSONDecodeError as e:
                            self.logger.warning(
                                "json_decode_failed", error=str(e))
                        except Exception as e:
                            self.logger.error(
                                "processing_failed", error=str(e))
            except Exception as e:
                self.logger.error("consumer_error", error=str(e))
