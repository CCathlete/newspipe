# src/domain/services/data_ingestion.py

from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any, Final

from returns.io import IOFailure, IOResult, IOResultE, IOSuccess
from returns.future import FutureResult, FutureResultE, future_safe
from returns.result import Failure, Success
from structlog.typing import FilteringBoundLogger

from domain.models import BronzeRecord, RelevancePolicy
from domain.interfaces import AIProvider, StorageProvider

@dataclass(slots=True, frozen=True)
class IngestionPipeline:
    llm: AIProvider
    lakehouse: StorageProvider
    logger: FilteringBoundLogger
    buffer_size: int
    relevance_policy: RelevancePolicy
    _buffer: list[BronzeRecord] = field(default_factory=list, init=False)

    @future_safe
    async def ingest_if_relevant(self, data: dict[str, Any]) -> None:
        log: Final = self.logger.bind(url=data.get("url"))

        content: str = data.get("chunk", "")
        language: str = data.get("language", "")

        # 1. Heuristic Filter (Cheap)
        if not self.relevance_policy.validate_content(content):
            log.debug("chunk_rejected_by_policy_heuristics")
            return

        # 2. LLM Filter (Expensive)
        is_relevant_future: FutureResultE[bool] = self.llm.is_relevant(
            text=content,
            language=language,
            policy_description=self.relevance_policy.description
        )
        is_relevant_io: IOResultE[bool] = await is_relevant_future.awaitable()
        match is_relevant_io:
            case IOSuccess(Success(is_relevant)):
                if is_relevant == True:
                    self.logger.info("Chunk %s is relevant, ingesting further.", content)

                    is_created: FutureResultE = self._create_record(data)
                    io_is_created: IOResultE = await is_created

                    match io_is_created:
                        case IOSuccess(Success(bronzerecord)):
                            self._buffer.append(bronzerecord)
                            flush_io: IOResultE[int] = await self._check_and_flush(log)
                            match flush_io:
                                case IOSuccess(Success(did_it_flush)):
                                    if did_it_flush > 0:
                                        self.logger.info(
                                            "Record starting with %s was successfully pushed to the lakehouse.",
                                            str(bronzerecord)[:10]
                                        )
                                    else:
                                        self.logger.error("Record buffer is empty.")

                                case IOFailure(Failure(e)):
                                    self.logger.error("Failed to flush record: %s", e)
                            return

                        case _: raise
            case IOFailure(Failure(e)):
                self.logger.error("Failed to get relevance from llm: %s", e)


    @future_safe
    async def _create_record(self, data: dict[str, Any]) -> BronzeRecord:
        return BronzeRecord(
            chunk_id=f"{data['url']}_{datetime.now(UTC).timestamp()}",
            source_url=data["url"],
            content=data["chunk"],
            language=data.get("language", "en"),
            ingested_at=datetime.now(UTC).timestamp(),
        )

    @future_safe
    async def _check_and_flush(self, log: FilteringBoundLogger) -> int:
        if len(self._buffer) >= self.buffer_size:
            io_res: IOResult[int, Exception] = await self.flush_all(log).awaitable()
            match io_res:
                case IOSuccess(Success(res)):
                    return res
                case IOFailure(Failure(e)):
                    raise e
        return 0

    @future_safe
    async def flush_all(self, log: FilteringBoundLogger) -> int:
        if not self._buffer:
            return 0

        records: list[BronzeRecord] = list(self._buffer)
        self._buffer.clear()

        write_operation_result: FutureResult[int, Exception] = await self.lakehouse.write_records(records)
        io_res: IOResult = await write_operation_result.awaitable()
        
        match io_res:
            case IOSuccess(Success(res)):
                count: int = res
                log.info("buffer_flushed", count=count)
                return count

            case IOFailure(Failure(e)):
                log.error("flush_failed", error=str(e))
                raise e

            case _:
                raise RuntimeError("Inconsistent IOResult state")





