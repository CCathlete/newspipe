# src/domain/services/scraper.py

import json
from dataclasses import dataclass
from typing import Callable
from urllib.parse import ParseResult, urljoin, urlparse

from returns.future import FutureResultE, future_safe
from returns.io import IOFailure, IOResultE, IOSuccess
from returns.result import Failure, Success
from structlog.typing import FilteringBoundLogger

from domain.interfaces import (
    ChunkingStrategy,
    Crawler,
    CrawlerResult,
    CrawlerRunConfig,
    KafkaPort,
)
from domain.models import BronzeRecord, TraversalRules

@dataclass(slots=True, frozen=True)
class StreamScraper:
    logger: FilteringBoundLogger
    kafka_provider: KafkaPort
    crawler_factory: Callable[[], Crawler]
    traversal_rules: TraversalRules
    run_config: CrawlerRunConfig
    strategy: ChunkingStrategy

    @future_safe
    async def initialize_and_seed(self, seeds: dict[str, list[str]], topics: list[str]) -> list[str]:
        infra_future: FutureResultE[list[str]] = self.kafka_provider.ensure_topics_exist(topics)
        infra_res: IOResultE[list[str]] = await infra_future.awaitable()
        
        match infra_res:
            case IOSuccess(Success(_)):
                for lang, urls in seeds.items():
                    for url in urls:
                        payload: bytes = json.dumps({"url": url, "language": lang}).encode("utf-8")
                        send_future: FutureResultE[None] = self.kafka_provider.send(topic="discovery_queue", value=payload)
                        await send_future.awaitable()
                return topics
            case IOFailure(Failure(e)):
                raise e
            case _:
                raise RuntimeError("Inconsistent Kafka state during initialization")

    @future_safe
    async def deep_crawl(
        self,
        url: str,
        language: str,
        discovery_topics: list[str] = ["discovery_queue"],
        chunks_topic: str = "raw_chunks",
        depth: int = 0
    ) -> None:
        async with self.crawler_factory() as crawler:
            result: CrawlerResult = await crawler.arun(url=url, config=self.run_config)

            if not result.success:
                raise RuntimeError(result.error_message or "Crawl failed")

            # 1. Deterministic Link Discovery (now uses url to resolve relatives)
            discovery_future: FutureResultE[None] = self._discover_links(
                result, url, discovery_topics, language, depth
            )
            await discovery_future.awaitable()

            # 2. Content Chunking & Publishing
            if not result.markdown:
                raise ValueError(f"No content retrieved from {url}")

            publish_future: FutureResultE[None] = self._publish_chunks(result.markdown, self.strategy, url, language, chunks_topic)
            await publish_future.awaitable()
            
            return None

    @future_safe
    async def _publish_chunks(
        self, 
        content: str, 
        strategy: ChunkingStrategy, 
        url: str, 
        language: str, 
        topic: str
    ) -> None:

        for idx, chunk in enumerate(strategy.chunk(content)):
            chunk = chunk.strip()
            if len(chunk) <= 100:
                continue

            # Create a BronzeRecord instance
            record = BronzeRecord(
                chunk_id=f"{url}::{idx}",  # deterministic ID per source + chunk index
                source_url=url,
                content=chunk,
                language=language
            )

            # Serialize to JSON
            payload: bytes = record.model_dump_json().encode("utf-8")

            # Send to Kafka
            send_res_future: FutureResultE[None] = self.kafka_provider.send(
                topic=topic,
                value=payload,
                key=url.encode("utf-8")
            )
            send_res_io: IOResultE[None] = await send_res_future.awaitable()

            match send_res_io:
                case IOSuccess(Success(_)):
                    pass
                case IOFailure(Failure(e)):
                    self.logger.error("chunk_publish_failed", url=url, error=str(e))

    def __normalize(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"

    @future_safe
    async def _discover_links(
            self, 
            result: CrawlerResult, 
            base_url: str, 
            topics: list[str], 
            language: str,
            current_depth: int  # Passed from deep_crawl.
        ) -> None:
            internal_links: list[dict[str, str]] = result.links.get("internal", [])


            
            for link_properties in internal_links:
                href = link_properties.get("href")
                if not href:
                    continue

                absolute: str = urljoin(base_url, href)
                link: str = self.__normalize(absolute)
                self.logger.debug("link_discovered", link=link, depth=current_depth + 1)

                parsed: ParseResult = urlparse(link)
                link_for_check: str = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


                # Pass depth to the validation check
                validity_future: FutureResultE[bool] = self._is_valid_navigation(
                    link_for_check, 
                    current_depth + 1
                )
                validity_io_monad: IOResultE[bool] = await validity_future.awaitable()
                
                match validity_io_monad:
                    case IOSuccess(Success(True)):
                        # Now the payload includes depth so the next crawler knows where it is
                        payload: bytes = json.dumps({
                            "url": link, 
                            "language": language,
                            "depth": current_depth + 1
                        }).encode("utf-8")
                        
                        for topic in topics:
                            send_future: FutureResultE[None] = self.kafka_provider.send(
                                topic=topic, 
                                value=payload,
                                key=link.encode("utf-8")
                            )
                            await send_future.awaitable()
                            
                    case IOSuccess(Success(False)):
                        continue # Blocked or reached max depth
                        
                    case IOFailure(Failure(e)):
                        self.logger.error("nav_validation_failed", url=link, error=str(e))

    @future_safe
    async def _is_valid_navigation(self, url: str, current_depth: int) -> bool:
            """
            Directly utilizes the TraversalRules model logic to determine 
            if the crawler should proceed to this URL.
            """
            # Calling the traversal policy's method directly
            return self.traversal_rules.is_path_allowed(
                url=url, 
                current_depth=current_depth
            )
