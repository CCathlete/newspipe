# src/infrastructure/scraper.py

from dataclasses import dataclass
from structlog.typing import FilteringBoundLogger
from collections.abc import AsyncIterator
from returns.result import Result, Success, Failure
from crawl4ai import (
    AsyncWebCrawler,
    CrawlerRunConfig,
    cache_context,
    markdown_generation_strategy
)
from crawl4ai.chunking_strategy import OverlappingWindowChunking


@dataclass(slots=True, frozen=True)
class StreamScraper:
    logger: FilteringBoundLogger
    # We define the chunking logic inside the crawler config
    # 500 words is roughly 2-3 paragraphs, good for geopolitical context
    chunk_size: int = 500

    async def scrape_and_chunk(
        self,
        url: str
    ) -> AsyncIterator[Result[str, Exception]]:
        log = self.logger.bind(url=url)
        log.info("Starting Crawl4AI semantic scraping.")

        # Configure semantic chunking
        run_config = CrawlerRunConfig(
            cache_mode=cache_context.CacheMode.BYPASS,
            chunking_strategy=OverlappingWindowChunking(
                window_size=self.chunk_size,
                overlap=50
            ),
            markdown_generator=markdown_generation_strategy
            .DefaultMarkdownGenerator(options={"ignore_links": False})
        )

        try:
            async with (crawler := AsyncWebCrawler()):
                result = await crawler.arun(url=url, config=run_config)

                if not result.success:
                    yield Failure(RuntimeError(result.error_message))
                    return

                # Crawl4AI provides semantic chunks in the metadata
                # We turn these into your expected stream
                chunks = result.metadata.get("chunks", [])

                if not chunks:
                    # Fallback: if no strategy chunks found, yield the whole markdown
                    yield Success(result.markdown)
                    return

                for chunk in chunks:
                    # Filter out purely technical/short chunks site-agnostically
                    if len(chunk) > 100:
                        yield Success(chunk)

        except Exception as e:
            log.error("Crawl4AI failed", error=str(e))
            yield Failure(e)
