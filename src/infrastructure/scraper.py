# src/infrastructure/scraper.py

import json
import asyncio
from typing import Callable
from dataclasses import dataclass
from structlog.typing import FilteringBoundLogger
from collections.abc import AsyncIterator
from returns.result import Result, Success, Failure

from ..domain.interfaces import (
    Crawler,
    CrawlerResult,
    ChunkingStrategy,
    CrawlerRunConfig,
    KafkaProvider
)


@dataclass(slots=True, frozen=True)
class StreamScraper:
    logger: FilteringBoundLogger
    kafka_consumer: KafkaProvider
    crawler_factory: Callable[[], Crawler]

    async def scrape_and_chunk(
        self,
        url: str,
        strategy: ChunkingStrategy,
        run_config: CrawlerRunConfig,
    ) -> AsyncIterator[Result[str, Exception]]:
        log = self.logger.bind(url=url)
        log.info("Starting Crawl4AI semantic scraping.")

        try:
            async with self.crawler_factory() as crawler:
                result: CrawlerResult = await crawler.arun(
                    url=url,
                    config=run_config,
                )

                if not result.success:
                    yield Failure(RuntimeError(result.error_message or "Unknown crawl error"))
                    return

                # 2. Extract chunks explicitly using the strategy
                # If Crawl4AI didn't put them in metadata, we generate them from the markdown
                content_to_chunk = result.markdown

                if not content_to_chunk:
                    yield Failure(RuntimeError("No content retrieved from URL"))
                    return

                # The strategy object has a chunk method
                chunks: list[str] = strategy.chunk(content_to_chunk)

                if not chunks:
                    yield Success(content_to_chunk)
                    return

                for chunk in chunks:
                    # Filter out noise/short fragments
                    if len(chunk.strip()) > 100:
                        yield Success(chunk.strip())

        except Exception as e:
            log.error("Crawl4AI failed", error=str(e))
            yield Failure(e)

    async def process_discovery_queue(
        self,
        strategy: ChunkingStrategy,
        run_config: CrawlerRunConfig,
        topic: str = "discovery_queue",
    ) -> AsyncIterator[Result[str, Exception]]:
        log = self.logger.bind(topic=topic)
        log.info("Starting discovery queue processing")

        # Subscribe to the topic if not already subscribed
        self.kafka_consumer.subscribe(topic)

        while True:
            try:
                # Get messages from Kafka with a timeout
                messages = await self.kafka_consumer.getmany(timeout_ms=1000)

                if not messages:
                    continue

                for tp, message_list in messages.items():
                    for message in message_list:
                        try:
                            # Handling both raw bytes and deserialized values.
                            if isinstance(message.value, bytes):
                                data = json.loads(
                                    message.value.decode('utf-8'))
                            else:
                                data = message.value

                            if not isinstance(data, dict):
                                log.warning(
                                    "Message value is not a dictionary", value=data)
                                continue

                            url = data.get("url")
                            if not url:
                                log.warning(
                                    "Invalid message format - missing URL", message=data)
                                continue

                            log.info("Processing discovered URL", url=url)
                            async for result in self.scrape_and_chunk(
                                url=url,
                                strategy=strategy,
                                run_config=run_config,
                            ):
                                yield result

                        except json.JSONDecodeError as e:
                            log.error("Failed to decode message", error=str(e))
                            yield Failure(e)

                        except Exception as e:
                            log.error(
                                "Unexpected error processing message", error=str(e))
                            yield Failure(e)

            except Exception as e:
                log.error("Error in discovery queue processing", error=str(e))
                yield Failure(e)
                await asyncio.sleep(1)  # Small delay before retrying
