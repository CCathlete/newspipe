# src/domain/services/data_ingestion.py

from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any, Final

from returns.io import IOFailure, IOResult, IOResultE, IOSuccess
from returns.future import FutureResult, FutureResultE, future_safe
from returns.result import Failure, Success
from structlog.typing import FilteringBoundLogger

from domain.models import BronzeRecord, RelevancePolicy
from domain.interfaces import AIPort, StoragePort

@dataclass(slots=True)
class IngestionPipeline:
    llm: AIPort
    lakehouse: StoragePort
    logger: FilteringBoundLogger
    buffer_size: int
    relevance_policy: RelevancePolicy
    _buffer: dict[str, list[BronzeRecord]] = field(default_factory=dict, init=False)


    @future_safe
    async def ingest_if_relevant(self, data: dict[str, Any]) -> None:
        log: Final = self.logger.bind(url=data.get("url"))

        content: str = data.get("content", "")
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
        flush: bool = False
        match is_relevant_io:
            case IOSuccess(Success(is_relevant)):
                if is_relevant == True:
                    self.logger.info("Chunk %s is relevant, ingesting further.", content[:100])
                    flush = True

            case IOFailure(Failure(e)):
                self.logger.error("Failed to get relevance from llm: %s", e)
                flush = True

        if flush == True:
            is_created: FutureResultE = self._create_record(data)
            io_is_created: IOResultE = await is_created.awaitable()

            match io_is_created:
                case IOSuccess(Success(bronzerecord)):
                    self._buffer.setdefault(bronzerecord.chunk_id, []).append(bronzerecord)

                    flush_io: IOResultE[int] = await self._check_and_flush_group(bronzerecord.chunk_id, log).awaitable()
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


    @future_safe
    async def _create_record(self, data: dict[str, Any]) -> BronzeRecord:
        return BronzeRecord(
            chunk_id = data.get("chunk_id") or f"{data['url']}_{datetime.now(UTC).timestamp()}",
            source_url=data["url"],
            content=data.get("chunk", "").strip(),
            language=data.get("language", "en"),
            ingested_at=datetime.now(UTC).timestamp(),
        )


    @future_safe
    async def _check_and_flush_group(self, key: str, log: FilteringBoundLogger) -> int:
        """
        Flush a logical group if it exists.
        """
        if key not in self._buffer:
            return 0

        # Optional safety: flush if group exceeds buffer_size
        if len(self._buffer[key]) >= self.buffer_size:
            io_flushed_count: IOResultE[int] = await self._flush_group(key, log).awaitable()
            match io_flushed_count:
                case IOSuccess(Success(flushed_count)):
                    return flushed_count
                case IOFailure(Failure(error)):
                    raise error
        return 0

    @future_safe
    async def _flush_group(self, key: str, log: FilteringBoundLogger) -> int:
        if key not in self._buffer or not self._buffer[key]:
            return 0

        records: list[BronzeRecord] = self._buffer.pop(key)
        write_res: FutureResult[int, Exception] = self.lakehouse.write_records(records)
        io_res: IOResult[int, Exception] = await write_res.awaitable()

        match io_res:
            case IOSuccess(Success(count)):
                log.info("Logical group flushed", key=key, count=count)
                return count
            case IOFailure(Failure(e)):
                log.error("Flush failed for group %s: %s", key, str(e))
                raise e

        return 0





