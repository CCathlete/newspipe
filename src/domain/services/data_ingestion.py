from dataclasses import dataclass, field
from structlog.typing import FilteringBoundLogger
from returns.result import Success, Failure, Result
from ..models import BronzeRecord, BronzeTagResponse
from ..interfaces import ScraperProvider, StorageProvider, AIProvider


@dataclass(slots=True, frozen=True)
class IngestionPipeline:
    scraper: ScraperProvider
    ollama: AIProvider
    lakehouse: StorageProvider

    logger: FilteringBoundLogger = field(init=False)

    async def execute(self, url: str) -> Result[int, Exception]:
        log = self.logger.bind(url=url)
        records: list[BronzeRecord] = []
        current_article_id: str = "INIT"  # Will be updated by LLM

        # Consume the stream
        async for chunk_result in await self.scraper.scrape_and_chunk(url):
            # Monadic bind_async: Only call Ollama if scraping was successful
            # We use Success(current_article_id) to pipe the state forward

            tag_result: Result[BronzeTagResponse, Exception]

            match chunk_result:
                case Success(chunk):
                    tag_result = await self.ollama.tag_chunk(
                        article_id=current_article_id,
                        content=chunk
                    )

                    match tag_result:
                        case Success(tag):
                            current_article_id = tag.article_id
                            records.append(BronzeRecord(
                                chunk_id=tag.chunk_id,
                                article_id=tag.article_id,
                                content=chunk_result.unwrap(),  # Safe because tag_result succeeded
                                control_action=tag.control_action
                            ))

                        case _: pass

                case Failure(e):
                    log.error("pipeline_step_failed", error=str(e))
                    # We continue to next chunk even if one fails.
                    continue

                case _: pass

        # Final terminal action: Write to Lakehouse
        return self.lakehouse.write_records(records)
