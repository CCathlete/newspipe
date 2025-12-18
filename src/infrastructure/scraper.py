# src/infrastructure/scraper.py

from dataclasses import dataclass
import httpx
from collections.abc import AsyncIterator
from returns.result import Result, Success, Failure


@dataclass(slots=True, frozen=True)
class StreamScraper:
    client: httpx.AsyncClient
    chunk_size: int = 4096

    async def scrape_and_chunk(
        self,
        url: str
    ) -> AsyncIterator[Result[str, Exception]]:
        """
        An Async Generator yielding Results. 
        Note: We don't use @future_safe here because this is a stream, not a single future.
        """
        try:
            async with self.client.stream("GET", url) as response:
                if response.status_code != 200:
                    yield Failure(RuntimeError(f"HTTP {response.status_code}"))
                    return

                async for chunk in response.aiter_text():
                    for i in range(0, len(chunk), self.chunk_size):
                        yield Success(chunk[i: i + self.chunk_size])

        except Exception as e:
            # Catching the exception and yielding it as a Failure
            # ensures the consumer can handle the error without a crash.
            yield Failure(e)
