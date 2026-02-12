# src/domain/services/scraper.py

import json
from dataclasses import dataclass
from urllib.parse import ParseResult, urljoin, urlparse

from crawl4ai.adaptive_crawler import CrawlState
from crawl4ai.models import Link, StringCompatibleMarkdown
from returns.future import FutureResultE, future_safe
from returns.io import IOFailure, IOResultE, IOSuccess
from returns.result import Failure, Success
from structlog.typing import FilteringBoundLogger

from domain.interfaces import (
    ChunkingStrategy,
    AdaptiveCrawler,
    CrawlerRunConfig,
    KafkaPort,
)
from domain.models import BronzeRecord, TraversalRules

@dataclass(slots=True, frozen=True)
class StreamScraper:
    logger: FilteringBoundLogger
    kafka_provider: KafkaPort
    crawler: AdaptiveCrawler
    traversal_rules: TraversalRules
    run_config: CrawlerRunConfig
    strategy: ChunkingStrategy
    query: str

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

        state: CrawlState = await self.crawler.digest(url=url, query=self.query)

        # Track crawl order
        state.crawl_order.append(url)

        # Publish content from knowledge base
        for crawl_result in state.knowledge_base:
            assert crawl_result.markdown
            await self._publish_chunks(
                content=crawl_result.markdown, 
                strategy=self.strategy, 
                url=crawl_result.url, 
                language=language, 
                topic=chunks_topic
            )

        # Discover and enqueue new links
        await self._discover_links(
            result=state,
            base_url=url, 
            topics=discovery_topics, 
            language=language, 
            current_depth=depth
        )

    @future_safe
    async def _publish_chunks(
        self, 
        content: str | StringCompatibleMarkdown, 
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

    @future_safe
    async def _discover_links(
            self, 
            result: CrawlState, 
            base_url: str, 
            topics: list[str], 
            language: str,
            current_depth: int  # Passed from deep_crawl.
        ) -> None:
            internal_links: list[Link] = result.pending_links


            
            for link_properties in internal_links:
                href: str | None = link_properties.href
                if not href:
                    continue

                
                base_netloc: str = urlparse(base_url).netloc
                absolute: str = urljoin(base_url, href)
                parsed_link: ParseResult = urlparse(absolute)

                # skip links outside the base repo/domain
                if parsed_link.netloc != base_netloc:
                    self.logger.debug("link_skipped_outside_base", link=absolute)
                    continue

                link: str = f"{parsed_link.scheme}://{parsed_link.netloc}{parsed_link.path.rstrip('/')}"
                self.logger.info("link_discovered", link=link, depth=current_depth + 1)

                # Pass depth to the validation check
                validity_future: FutureResultE[bool] = self._is_valid_navigation(
                    link, 
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
