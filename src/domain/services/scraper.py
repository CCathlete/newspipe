# src/domain/services/scraper.py

import json
from dataclasses import dataclass
from typing import Any, Callable, Final
from urllib.parse import urlparse
from collections.abc import AsyncIterator

from structlog.typing import FilteringBoundLogger
from returns.result import Result, Success, Failure
from aiokafka.structs import TopicPartition, ConsumerRecord

from domain.interfaces import (
    Crawler,
    CrawlerResult,
    ChunkingStrategy,
    CrawlerRunConfig,
    KafkaProvider
)
from domain.models import RelevancePolicy


@dataclass(slots=True, frozen=True)
class StreamScraper:
    logger: FilteringBoundLogger
    kafka_provider: KafkaProvider
    crawler_factory: Callable[[], Crawler]
    policy: RelevancePolicy

    async def process_from_topic(
        self,
        strategy: ChunkingStrategy,
        run_config: CrawlerRunConfig,
        topic: str = "discovery_queue",
    ) -> AsyncIterator[Result[str, Exception]]:
        log: Final = self.logger.bind(topic=topic, policy_name=self.policy.name)
        log.info("discovery_process_started")
        
        self.kafka_provider.subscribe([topic])

        while True:
            messages: dict[TopicPartition, list[ConsumerRecord[Any, Any]]]  = await self.kafka_provider.getmany(timeout_ms=1000)
            if not messages:
                continue

            for _, message_list in messages.items():
                for message in message_list:
                    match self._decode_payload(message.value):
                        case Success({"url": str(url), "language": str(lang)}):
                            if not self._is_valid_navigation(url):
                                log.debug("url_rejected_by_policy", url=url)
                                continue
                            
                            log.info("processing_discovered_url", url=url)
                            async for result in self.scrape_and_chunk(url, strategy, run_config, lang):
                                yield result
                                
                        case Success(malformed):
                            log.warning("malformed_payload_structure", payload=malformed)
                            
                        case Failure(e):
                            log.error("message_deserialization_failed", error=str(e))

    async def scrape_and_chunk(
        self,
        url: str,
        strategy: ChunkingStrategy,
        run_config: CrawlerRunConfig,
        language: str,
    ) -> AsyncIterator[Result[str, Exception]]:
        log: Final = self.logger.bind(url=url, language=language)
        
        try:
            async with self.crawler_factory() as crawler:
                result: CrawlerResult = await crawler.arun(url=url, config=run_config)

                if not result.success:
                    yield Failure(RuntimeError(result.error_message or "Crawl failed"))
                    return

                # Handle navigation/discovery: Feed valid links back to Kafka
                await self._discover_links(result, log)

                # Handle content: Chunk and yield
                match result.markdown:
                    case str(content) if len(content) > 0:
                        for chunk in strategy.chunk(content):
                            if len(chunk.strip()) > 100:
                                yield Success(chunk.strip())
                    case _:
                        yield Failure(ValueError(f"No content retrieved from {url}"))

        except Exception as e:
            log.error("scrape_and_chunk_failed", error=str(e))
            yield Failure(e)

    def _is_valid_navigation(self, url: str) -> bool:
        rules = self.policy.traversal
        parsed = urlparse(url)
        path = parsed.path.lower()

        if rules.allowed_domains and parsed.netloc not in rules.allowed_domains:
            return False

        if any(seg in path for seg in rules.blocked_path_segments):
            return False

        if rules.required_path_segments:
            return any(seg in path for seg in rules.required_path_segments)

        return True

    async def _discover_links(self, result: CrawlerResult, log: FilteringBoundLogger) -> None:
        """Extracts and filters links from crawl result, pushing them back to the discovery queue."""
        # Crawl4AI typically returns links in extracted_content or metadata
        links: list[str] = getattr(result, "links", [])
        
        valid_links = [l for l in links if self._is_valid_navigation(l)]
        
        for link in valid_links:
            payload: str = json.dumps({"url": link, "language": "en"})
            await self.kafka_provider.send(topic="discovery_queue", value=payload.encode("utf-8"))
        
        if valid_links:
            log.debug("links_discovered", count=len(valid_links))

    def _decode_payload(self, value: Any) -> Result[dict[str, Any], Exception]:
        match value:
            case bytes():
                return self._parse_json(value.decode("utf-8"))
            case str():
                return self._parse_json(value)
            case dict():
                return Success(value)
            case _:
                return Failure(TypeError(f"Unsupported message type: {type(value)}"))

    def _parse_json(self, value: str) -> Result[dict[str, Any], Exception]:
        try:
            data = json.loads(value)
            return Success(data) if isinstance(data, dict) else Failure(TypeError("Payload not a dict"))
        except json.JSONDecodeError as e:
            return Failure(e)
