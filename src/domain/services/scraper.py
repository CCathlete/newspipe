# src/domain/services/scraper.py

import json
from dataclasses import dataclass

from crawl4ai.adaptive_crawler import CrawlState
from crawl4ai.models import StringCompatibleMarkdown
from returns.future import FutureResultE, future_safe
from returns.io import IOFailure, IOResultE, IOSuccess
from returns.result import Failure, Success
from structlog.typing import FilteringBoundLogger

from domain.interfaces import ChunkingStrategy, AdaptiveCrawler, KafkaPort
from domain.models import BronzeRecord, TraversalRules


@dataclass(slots=True, frozen=True)
class StreamScraper:
    logger: FilteringBoundLogger
    kafka_provider: KafkaPort
    crawler: AdaptiveCrawler
    traversal_rules: TraversalRules
    strategy: ChunkingStrategy
    query: str

    # ------------------------------------------------------------------ #
    # Seeding
    # ------------------------------------------------------------------ #

    @future_safe
    async def initialize_and_seed(
        self,
        seeds: dict[str, list[str]],
        topics: list[str],
        discovery_topic_name: str = "discovery_queue",
    ) -> list[str]:

        infra_future: FutureResultE[list[str]] = self.kafka_provider.ensure_topics_exist(topics)
        infra_res: IOResultE[list[str]] = await infra_future.awaitable()

        match infra_res:
            case IOSuccess(Success(_)):
                for language, urls in seeds.items():
                    for url in urls:

                        if not self.traversal_rules.is_path_allowed(url=url, current_depth=0):
                            continue

                        payload: bytes = json.dumps({
                            "url": url,
                            "language": language,
                        }).encode()

                        send_future: FutureResultE[None] = self.kafka_provider.send(
                            topic=discovery_topic_name,
                            value=payload,
                            key=url.encode(),
                        )
                        await send_future.awaitable()

                return topics

            case IOFailure(Failure(e)):
                raise e

            case _:
                raise RuntimeError("Kafka initialization inconsistent")

    # ------------------------------------------------------------------ #
    # Adaptive session
    # ------------------------------------------------------------------ #

    @future_safe
    async def deep_crawl(
        self,
        url: str,
        language: str,
        chunks_topic: str,
    ) -> dict[str, float]:

        state: CrawlState = await self.crawler.digest(
            start_url=url,
            query=self.query,
        )

        published: int = 0

        for result in state.knowledge_base:
            assert result.markdown is not None

            await self._publish_chunks(
                content=result.markdown,
                url=result.url,
                language=language,
                topic=chunks_topic,
            )
            published += 1

        self.logger.info(
            "adaptive_session_completed",
            seed=url,
            pages=len(state.knowledge_base),
            chunks=published,
            metrics=state.metrics,
        )

        return state.metrics

    # ------------------------------------------------------------------ #
    # Chunk publishing
    # ------------------------------------------------------------------ #

    @future_safe
    async def _publish_chunks(
        self,
        content: str | StringCompatibleMarkdown,
        url: str,
        language: str,
        topic: str,
    ) -> None:

        for idx, chunk in enumerate(self.strategy.chunk(content)):
            text: str = chunk.strip()
            if len(text) <= 100:
                continue

            record: BronzeRecord = BronzeRecord(
                chunk_id=f"{url}::{idx}",
                source_url=url,
                content=text,
                language=language,
            )

            payload: bytes = record.model_dump_json().encode()

            send_future: FutureResultE[None] = self.kafka_provider.send(
                topic=topic,
                value=payload,
                key=url.encode(),
            )

            send_res: IOResultE[None] = await send_future.awaitable()

            match send_res:
                case IOSuccess(Success(_)):
                    pass
                case IOFailure(Failure(e)):
                    self.logger.error("chunk_publish_failed", url=url, error=str(e))

