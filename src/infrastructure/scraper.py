# src/infrastructure/scraper.py

from typing import Callable
from dataclasses import dataclass
from structlog.typing import FilteringBoundLogger
from collections.abc import AsyncIterator
from returns.result import Result, Success, Failure

from ..domain.interfaces import Crawler, CrawlerResult, ChunkingStrategy, CrawlerRunConfig


@dataclass(slots=True, frozen=True)
class StreamScraper:
    logger: FilteringBoundLogger
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
