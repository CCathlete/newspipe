# src/infrastructure/scraper.py

import httpx
from dataclasses import dataclass
from structlog.typing import FilteringBoundLogger
from collections.abc import AsyncIterator
from returns.result import Result, Success, Failure
from returns.maybe import Maybe, Some, Nothing
from selectolax.parser import HTMLParser


@dataclass(slots=True, frozen=True)
class StreamScraper:
    client: httpx.AsyncClient
    logger: FilteringBoundLogger
    chunk_size: int = 4096

    def _clean_chunk(self, html_content: str) -> Maybe[str]:
        tree = HTMLParser(html_content)

        for tag in tree.css("script, style, noscript, link, meta, svg, path"):
            tag.decompose()

        cleaned = tree.text(separator=" ", strip=True)

        return Some(cleaned) if len(cleaned) > 20 else Nothing

    async def scrape_and_chunk(
        self,
        url: str
    ) -> AsyncIterator[Result[str, Exception]]:
        log = self.logger.bind(url=url)

        try:
            log.info("Starting streaming scraped data chunks.")

            async with self.client.stream("GET", url, follow_redirects=True) as response:
                if response.status_code != 200:
                    log.warning("Streaming failed",
                                status_code=response.status_code)
                    yield Failure(RuntimeError(f"HTTP {response.status_code}"))
                    return

                logged_success: bool = False
                async for raw_chunk in response.aiter_text():
                    for i in range(0, len(raw_chunk), self.chunk_size):
                        segment = raw_chunk[i: i + self.chunk_size]

                        match self._clean_chunk(segment):
                            case Some(clean_text):
                                if not logged_success:
                                    log.info("Streaming started successfully.")
                                    logged_success = True
                                yield Success(clean_text)

                            case Nothing:
                                log.info(
                                    "No clean text found in chunk.",
                                    result=str(Nothing),
                                )
                                continue

        except Exception as e:
            log.error("Streaming failed", error=str(e))
            yield Failure(e)
