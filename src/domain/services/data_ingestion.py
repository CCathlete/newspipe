# src/domain/services/data_ingestion.py

from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any, Final

from returns.io import IOFailure, IOResult, IOResultE, IOSuccess
from returns.future import FutureResult, FutureResultE, future_safe
from returns.result import Failure, Success
from structlog.typing import FilteringBoundLogger

from domain.models import BronzeRecord
from domain.interfaces import StorageProvider

@dataclass(slots=True, frozen=True)
class IngestionPipeline:
    lakehouse: StorageProvider
    logger: FilteringBoundLogger
    buffer_size: int = 10
    _buffer: list[BronzeRecord] = field(default_factory=list, init=False)

    @future_safe
    async def ingest_relevant_chunk(self, data: dict[str, Any]) -> None:
        log: Final = self.logger.bind(url=data.get("url"))

        is_created: FutureResultE = self._create_record(data)
        io_is_created: IOResultE = await is_created

        match io_is_created:
            case IOSuccess(Success(was_it)):
                self._buffer.append(was_it)
                self._check_and_flush(log)
                return

            case _: raise


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





