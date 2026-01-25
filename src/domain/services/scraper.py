# src/domain/services/scraper.py

import json
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import urlparse

from returns.future import FutureResultE, future_safe
from returns.io import IOFailure, IOResultE, IOSuccess
from returns.result import Failure, Success
from structlog.typing import FilteringBoundLogger

from domain.interfaces import (
    ChunkingStrategy,
    Crawler,
    CrawlerResult,
    CrawlerRunConfig,
    KafkaProvider,
)
from domain.models import TraversalRules

@dataclass(slots=True, frozen=True)
class StreamScraper:
    logger: FilteringBoundLogger
    kafka_provider: KafkaProvider
    crawler_factory: Callable[[], Crawler]
    traversal_rules: TraversalRules

    @future_safe
    async def initialize_and_seed(self, seeds: dict[str, list[str]], topics: list[str]) -> list[str]:
        # Infrastructure Check
        infra_future: FutureResultE[list[str]] = self.kafka_provider.ensure_topics_exist(topics)
        infra_res: IOResultE[list[str]] = await infra_future.awaitable()
        
        match infra_res:
            case IOSuccess(Success(_)):
                # Seed Injection
                for lang, urls in seeds.items():
                    for url in urls:
                        payload = json.dumps({"url": url, "language": lang}).encode("utf-8")
                        # Using the discovery topic for initial seeds
                        await self.kafka_provider.send(topic="discovery_queue", value=payload)
                return topics
            case IOFailure(Failure(e)):
                raise e
            case _:
                raise RuntimeError("Inconsistent Kafka state")

    @future_safe
    async def deep_crawl(
        self,
        url: str,
        strategy: ChunkingStrategy,
        run_config: CrawlerRunConfig,
        language: str,
        discovery_topics: list[str] = ["discovery_queue"],
        chunks_topic: str = "raw_chunks",
    ) -> bool:
        async with self.crawler_factory() as crawler:
            result: CrawlerResult = await crawler.arun(url=url, config=run_config)

            if not result.success:
                raise RuntimeError(result.error_message or "Crawl failed")

            # 1. Deterministic Link Discovery
            await self._discover_links(result, discovery_topics, language)

            # 2. Content Chunking & Publishing
            if not result.markdown:
                raise ValueError(f"No content retrieved from {url}")

            await self._publish_chunks(result.markdown, strategy, url, language, chunks_topic)
            return True

    async def _publish_chunks(
        self, 
        content: str, 
        strategy: ChunkingStrategy, 
        url: str, 
        language: str, 
        topic: str
    ) -> None:
        for chunk in strategy.chunk(content):
            if len(chunk.strip()) > 100:
                payload = json.dumps({
                    "url": url,
                    "chunk": chunk.strip(),
                    "language": language
                }).encode("utf-8")
                
                send_res_future: FutureResultE[bool] =  self.kafka_provider.send(topic=topic, value=payload)
                send_res_io: IOResultE[bool] = await send_res_future.awaitable()
                
                match send_res_io:
                    case IOSuccess(Success(_)):
                        pass
                    case IOFailure(Failure(e)):
                        self.logger.error("chunk_publish_failed", url=url, error=str(e))

    @future_safe
    async def _discover_links(self, result: CrawlerResult, topics: list[str], language: str) -> None:

        # TODO: result has no links attribute currently.
        links: list[str] = getattr(result, "links", [])
        valid_links: list[str] = []

        for link in links:
            validity_future: FutureResultE[bool] = self._is_valid_navigation(link)
            validity_io_monad: IOResultE[bool]= await validity_future.awaitable()
            match validity_io_monad:
                case IOSuccess(Success(is_valid_link)):
                    if is_valid_link == True:
                        valid_links.append(link)

                case IOFailure(Failure(e)):
                    self.logger.error("Failure in validating link %s: %s", link, e)
                    raise e

        
        for link in valid_links:
            payload: bytes = json.dumps({"url": link, "language": language}).encode("utf-8")
            for topic in topics:
                publish_future: FutureResultE[bool] = self.kafka_provider.send(topic=topic, value=payload)
                publish_io_monad: IOResultE[bool] = await publish_future.awaitable()

                match publish_io_monad:
                    case IOSuccess(Success(_)):
                        self.logger.info("discovery_link_published", url=link, topic=topic)
                    case IOFailure(Failure(e)):
                        self.logger.error("discovery_link_failed", url=link, error=str(e))

    @future_safe
    async def _is_valid_navigation(self, url: str) -> bool:
        rules: TraversalRules = self.traversal_rules
        path: str = urlparse(url).path.lower()
        if any(seg in path for seg in rules.blocked_path_segments):
            return False
        return not rules.required_path_segments or any(seg in path for seg in rules.required_path_segments)



