# src/infrastructure/lakehouse.py

import asyncio
from dataclasses import dataclass
from structlog.typing import FilteringBoundLogger
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import Row
from returns.future import future_safe
from returns.maybe import Maybe
from typing import Never, Final
from urllib.parse import urlparse

from domain.models import BronzeRecord
from domain.interfaces import SparkSessionInterface

def _sanitize(text: str) -> str:
    return text.replace("/", "_").replace(":", "_").replace(".", "_")

def _url_base_part(url: str) -> str:
    parsed: Final = urlparse(url)
    netloc: Final = _sanitize(parsed.netloc or "unknown")
    path: Final = _sanitize(parsed.path.strip("/") or "root")
    return f"{netloc}_{path}"

@dataclass(slots=True, frozen=True)
class LakehouseConnector:
    spark: SparkSessionInterface
    logger: FilteringBoundLogger
    bucket_path: str = "s3a://lakehouse/bronze/tagged_chunks"

    @property
    def path(self) -> str:
        return self.bucket_path.rstrip("/") + "/"

    def _convert_embedding(self, embedding: Maybe[list[float]]) -> list[float] | None:
        match embedding:
            case Maybe(value) if value in [None, Never]:
                return None
            case Maybe(value):
                try:
                    assert isinstance(value, list)
                    return [float(x) for x in value]
                except (TypeError, ValueError):
                    return None

    @future_safe
    async def write_records(self, records: list[BronzeRecord]) -> int:
        if not records:
            return 0

        log: Final = self.logger.bind(records_count=len(records))
        log.info("writing_to_lakehouse")

        data: Final = [Row(**r.model_dump()) for r in records]
        schema: Final = BronzeRecord.model_spark_schema()
        
        df: Final = self.spark.createDataFrame(data=data, schema=schema)
        df_partitioned: Final = df.withColumn(
            "ingested_date",
            F.from_unixtime(F.col("ingested_at"), "yyyy-MM-dd")
        )

        base_part: Final = _url_base_part(records[0].source_url)
        target_dir: Final = f"{self.path}{base_part}/"

        await asyncio.to_thread(
            self._execute_spark_write,
            df_partitioned,
            target_dir
        )

        log.info("records_written", path=target_dir)
        return len(records)

    def _execute_spark_write(self, df: DataFrame, target_dir: str) -> None:
        (
            df.write
            .mode("append")
            .partitionBy("ingested_date")
            .json(target_dir)
        )
