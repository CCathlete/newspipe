# src/infrastructure/lakehouse.py

from dataclasses import dataclass
from structlog.typing import FilteringBoundLogger
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, Row
from returns.result import safe
from returns.maybe import Maybe, Nothing
from typing import Never
from urllib.parse import urlparse

from ..domain.models import BronzeRecord
from ..domain.interfaces import SparkSessionInterface


def _sanitize(text: str) -> str:
    return text.replace("/", "_").replace(":", "_").replace(".", "_")


def _url_base_part(url: str) -> str:
    parsed = urlparse(url)
    netloc = _sanitize(parsed.netloc or "unknown")
    path = _sanitize(parsed.path.strip("/") or "root")
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
            case _:
                return None

    @safe
    def write_records(self, records: list[BronzeRecord]) -> int:
        log = self.logger.bind()
        if not records:
            log.warning("No records to write")
            return 0
        log.info("Writing records to lakehouse", records_count=len(records))

        data = [Row(**r.model_dump()) for r in records]
        schema = BronzeRecord.model_spark_schema()
        df = self.spark.createDataFrame(data=data, schema=schema)

        df_partitioned = df.withColumn(
            "ingested_date",
            F.from_unixtime(F.col("ingested_at"), "yyyy-MM-dd")
        )

        base_part = _url_base_part(records[0].source_url)
        target_dir = f"{self.path}{base_part}/"

        df_partitioned.write.mode("append").partitionBy(
            "ingested_date"
        ).json(target_dir)

        log.info("Records written to lakehouse", path=target_dir)
        return len(records)
