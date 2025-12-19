import json
from dataclasses import dataclass, field
from structlog.typing import FilteringBoundLogger
from returns.result import Success, Failure, Result
from returns.maybe import Some
from ..models import BronzeRecord, BronzeTagResponse
from ..interfaces import (
    ScraperProvider,
    StorageProvider,
    AIProvider,
    KafkaProvider,
)


@dataclass(slots=True, frozen=True)
class IngestionPipeline:
    scraper: ScraperProvider
    ollama: AIProvider
    lakehouse: StorageProvider
    kafka_producer: KafkaProvider

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
                                content=chunk_result.unwrap(),
                                control_action=tag.control_action
                            ))

                            match tag.control_action:
                                case "CLICKLINK":
                                    # 1. Extract the URL monadically into a local variable
                                    # This keeps the logic pure and resolves the 'self' scoping issue
                                    target_url = tag.metadata.map(
                                        lambda m: m.get("url")
                                    ).bind_optional(lambda url: url)

                                    # 2. Execute the side effect if we have a value
                                    # Using 'Success' here just to keep the pattern matching consistent
                                    match target_url:
                                        case Some(url_val):
                                            await self.kafka_producer.produce(
                                                topic="discovery_queue",
                                                value=json.dumps(
                                                    {"url": url_val}).encode()
                                            )
                                        case _: pass

                                case _: pass

                        case Failure(e):
                            log.warning("ollama_tagging_failed", error=str(e))

                        case _: pass

                case Failure(e):
                    log.error("pipeline_step_failed", error=str(e))
                    # We continue to next chunk even if one fails.
                    continue

                case _: pass

        # Final terminal action: Write to Lakehouse
        return self.lakehouse.write_records(records)
