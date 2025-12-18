# src/infrastructure/scraper.py

from dataclasses import dataclass, field
from structlog.typing import FilteringBoundLogger
import httpx
from collections.abc import AsyncIterator
from returns.result import Result, Success, Failure


@dataclass(slots=True, frozen=True)
class StreamScraper:
    client: httpx.AsyncClient
    chunk_size: int = 4096

    logger: FilteringBoundLogger = field(init=False)

    async def scrape_and_chunk(
        self,
        url: str
    ) -> AsyncIterator[Result[str, Exception]]:
        """
        An Async Generator yielding Results. 
        Note: We don't use @future_safe here because this is a stream, not a single future.
        """
        log = self.logger.bind()

        try:
            log.info("Starting streaming scraped data chunks.")

            async with self.client.stream("GET", url) as response:

                if response.status_code != 200:
                    log.warning(
                        "Streaming failed with status code %s", response.status_code
                    )

                    yield Failure(RuntimeError(f"HTTP {response.status_code}"))
                    return

                logged_success: bool = False
                # In case we get chunks larger than chunk_size, we rechunk
                # into chunk_size chunks and wrap it with a Success container.
                async for chunk in response.aiter_text():
                    for i in range(0, len(chunk), self.chunk_size):
                        if not logged_success:
                            log.info("Streaming started successfully.")
                        yield Success(chunk[i: i + self.chunk_size])

        except Exception as e:
            # Catching the exception and yielding it as a Failure
            # ensures the consumer can handle the error without a crash.
            log.error("Streaming failed", error=e)
            yield Failure(e)
