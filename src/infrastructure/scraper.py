import httpx
from collections.abc import AsyncIterator
from returns.result import Result, Success, Failure
# from ..domain.models import BronzeRecord

class StreamScraper:
    def __init__(self, client: httpx.AsyncClient, chunk_size: int = 4096):
        self.client = client
        self.chunk_size = chunk_size

    async def scrape_and_chunk(self, url: str) -> AsyncIterator[Result[str, Exception]]:
        try:
            async with self.client.stream("GET", url) as response:
                if response.status_code != 200:
                    yield Failure(RuntimeError(f"HTTP {response.status_code}"))
                    return

                async for chunk in response.aiter_text():
                    for i in range(0, len(chunk), self.chunk_size):
                        yield Success(chunk[i : i + self.chunk_size])
        except Exception as e:
            yield Failure(e)