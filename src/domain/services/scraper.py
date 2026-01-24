# src/domain/services/scraper.py

import json
from dataclasses import dataclass
from typing import Callable, Final
from urllib.parse import urlparse

from structlog.typing import FilteringBoundLogger
from returns.result import Result, Success, Failure

from domain.interfaces import (
    Crawler,
    CrawlerResult,
    ChunkingStrategy,
    CrawlerRunConfig,
    KafkaProvider
)
from domain.models import TraversalRules


@dataclass(slots=True, frozen=True)
class StreamScraper:
    logger: FilteringBoundLogger
    kafka_provider: KafkaProvider
    crawler_factory: Callable[[], Crawler]
    traversal_rules: TraversalRules

    async def deep_crawl(
        self,
        url: str,
        strategy: ChunkingStrategy,
        run_config: CrawlerRunConfig,
        language: str,
        discovery_topics: list[str] = ["discovery_queue"],
        chunks_topic: str = "raw_chunks",
    ) -> Result[bool, Exception]:
        """Performs crawl, discovers links, and publishes chunks to Kafka."""
        log: Final = self.logger.bind(url=url, language=language)
        
        try:
            async with self.crawler_factory() as crawler:
                result: CrawlerResult = await crawler.arun(url=url, config=run_config)

                if not result.success:
                    return Failure(RuntimeError(result.error_message or "Crawl failed"))

                # 1. Deterministic Link Discovery
                await self._discover_links(result, log, discovery_topics)

                # 2. Content Chunking & Publishing
                if result.markdown:
                    await self._publish_chunks(result.markdown, strategy, url, language, chunks_topic)
                    return Success(True)
                
                return Failure(ValueError(f"No content retrieved from {url}"))

        except Exception as e:
            log.info("deep_crawl_failed", error=str(e))
            return Failure(e)

    async def _publish_chunks(
        self, 
        content: str, 
        strategy: ChunkingStrategy, 
        url: str, 
        language: str, 
        topic: str
    ) -> None:
        """Chunks the markdown and sends each valid chunk to the raw_chunks topic."""
        for chunk in strategy.chunk(content):
            if len(chunk.strip()) > 100:
                payload = json.dumps({
                    "url": url,
                    "chunk": chunk.strip(),
                    "language": language
                }).encode("utf-8")
                
                # Monadic send check
                match await self.kafka_provider.send(topic=topic, value=payload):
                    case Success(_):
                        pass
                    case Failure(e):
                        self.logger.info("chunk_publish_failed", url=url, error=str(e))

    async def _discover_links(self, result: CrawlerResult, log: FilteringBoundLogger, topics: list[str]) -> None:
        links: list[str] = getattr(result, "links", [])
        valid_links = [l for l in links if self._is_valid_navigation(l)]
        
        for link in valid_links:
            payload = json.dumps({"url": link, "language": "en"}).encode("utf-8")
            for topic in topics:
                match await self.kafka_provider.send(topic=topic, value=payload):
                    case Success(_):
                        log.info("discovery_link_published", url=link, topic=topic)
                    case Failure(e):
                        log.info("discovery_link_failed", url=link, error=str(e))

    def _is_valid_navigation(self, url: str) -> bool:
        rules = self.traversal_rules
        path = urlparse(url).path.lower()
        if any(seg in path for seg in rules.blocked_path_segments):
            return False
        return not rules.required_path_segments or any(seg in path for seg in rules.required_path_segments)
