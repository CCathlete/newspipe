# src/infrastructure/lakehouse.py

from dataclasses import dataclass, field
from structlog.typing import FilteringBoundLogger
from pyspark.sql import SparkSession, DataFrame, functions as F
from returns.result import safe
from typing import cast, Any

from ..domain.models import BronzeRecord


@dataclass(slots=True, frozen=True)
class LakehouseConnector:
    spark: SparkSession
    bucket_path: str = "s3a://lakehouse/bronze/tagged_chunks"

    logger: FilteringBoundLogger = field(init=False)

    # Getter method, no setters, for setting we use dataclass.replace.

    @property
    def path(self) -> str:
        return self.bucket_path.rstrip("/") + "/"

    @safe
    def write_records(self, records: list[BronzeRecord],) -> int:

        log = self.logger.bind()

        if not records:
            log.warning("No records to write")
            return 0

        log.info("Writing records to lakehouse", records_count=len(records))
        # Convert Pydantic models to dictionaries for Spark
        data: list[dict[str, str]] = [r.model_dump() for r in records]

        df: DataFrame = self.spark.createDataFrame(
            cast(Any, data),
        )

        df_partitioned = df.withColumn(
            colName="ingested_date",
            col=F.from_unixtime(F.col("ingested_at"), "yyyy-MM-dd")
        )

        log.info("Writing JSON to partitioned lakehouse", path=self.path)

        df_partitioned.write.mode("append").partitionBy(
            "ingested_date"
        ).json(self.path)

        # Partitioning by source url for optimized geo-political analysis later
        df.write.mode("append").partitionBy(
            "source_url"
        ) .json(self.path)

        log.info("Records written to lakehouse")

        return len(records)
