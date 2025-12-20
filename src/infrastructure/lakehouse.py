# src/infrastructure/lakehouse.py

from dataclasses import dataclass
from structlog.typing import FilteringBoundLogger
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, Row
from returns.result import safe

from ..domain.models import BronzeRecord
from ..domain.interfaces import SparkSessionInterface


@dataclass(slots=True, frozen=True)
class LakehouseConnector:
    spark: SparkSessionInterface
    logger: FilteringBoundLogger
    bucket_path: str = "s3a://lakehouse/bronze/tagged_chunks"

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

        schema: StructType = BronzeRecord.model_spark_schema()
        data: list[Row] = [Row(**r.model_dump()) for r in records]

        df: DataFrame = self.spark.createDataFrame(data=data, schema=schema)

        df_partitioned: DataFrame = df.withColumn(
            colName="ingested_date",
            col=F.from_unixtime(F.col("ingested_at"), "yyyy-MM-dd")
        )

        log.info("Writing JSON to partitioned lakehouse", path=self.path)

        df_partitioned.write.mode("append").partitionBy(
            "ingested_date"
        ).json(self.path)

        log.info("Records written to lakehouse")

        return len(records)
