# src/domain/services/data_ingestion.py

from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any, Final

from returns.result import Failure, ResultE, Success, safe
from structlog.typing import FilteringBoundLogger

from domain.models import BronzeRecord
from domain.interfaces import KafkaPort, StoragePort

@dataclass(slots=True)
class IngestionPipeline:
    lakehouse: StoragePort
    logger: FilteringBoundLogger
    kafka_consumer: KafkaPort
    _buffer: dict[str, list[BronzeRecord]] = field(default_factory=dict, init=False)

    ingestion_topic_name: str = "for_ingestion"


    @safe
    def ingest_records(self) -> None:
        log: Final = self.logger.bind(service=self.__class__)

        data = ?

        content: str = data.get("content", "")
        language: str = data.get("language", "")


        is_created: ResultE = self._create_record(data)

        match is_created:
            case Success(bronzerecord):
                self._buffer.setdefault(bronzerecord.chunk_id, []).append(bronzerecord)

                flush_res: ResultE[int] = self._check_and_flush_group(bronzerecord.chunk_id, log)
                match flush_res:
                    case Success(did_it_flush):
                        if did_it_flush > 0:
                            self.logger.info(
                                "Record starting with %s was successfully pushed to the lakehouse.",
                                str(bronzerecord)[:10]
                            )
                        else:
                            self.logger.error("Record buffer is empty.")

                    case Failure(e):
                        self.logger.error("Failed to flush record: %s", e)
                return

            case _: raise


    @safe
    def _create_record(self, data: dict[str, Any]) -> BronzeRecord:
        return BronzeRecord(
            chunk_id = data.get("chunk_id") or f"{data['url']}_{datetime.now(UTC).timestamp()}",
            source_url=data["source_url"],
            content=data.get("content", "").strip(),
            language=data.get("language", "en"),
            ingested_at=datetime.now(UTC).timestamp(),
        )


    @safe
    def _check_and_flush_group(self, key: str, log: FilteringBoundLogger) -> int:
        """
        Flush a logical group if it exists.
        """
        if key not in self._buffer:
            return 0

        # Optional safety: flush if group exceeds buffer_size
        if len(self._buffer[key]) >= self.buffer_size:
            io_flushed_count: ResultE[int] = self._flush_group(key, log)
            match io_flushed_count:
                case Success(flushed_count):
                    return flushed_count
                case Failure(error):
                    raise error
        return 0

    @safe
    def _flush_group(self, key: str, log: FilteringBoundLogger) -> int:
        if key not in self._buffer or not self._buffer[key]:
            return 0

        records: list[BronzeRecord] = self._buffer.pop(key)
        write_res: ResultE = self.lakehouse.write_records(records)

        match write_res:
            case Success(count):
                log.info("Logical group flushed", key=key, count=count)
                return count
            case Failure(e):
                log.error("Flush failed for group %s: %s", key, str(e))
                raise e

        return 0





