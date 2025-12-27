# src/infrastructure/lakehouse.py

from dataclasses import dataclass
from structlog.typing import FilteringBoundLogger
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, Row
from returns.result import safe
from returns.maybe import Maybe, Nothing
from typing import Never

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

    def _convert_embedding(self, embedding: Maybe[list[float]]) -> list[float] | None:
        """Convert Maybe embedding to a PySpark-serializable format using pattern matching"""
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
    def write_records(self, records: list[BronzeRecord],) -> int:

        log = self.logger.bind()

        if not records:
            log.warning("No records to write")
            return 0

        log.info("Writing records to lakehouse", records_count=len(records))

        try:

            # Converting records to dict and handle embeddings.
            data: list[Row] = []
            for record in records:
                record_dict = record.model_dump()
                if 'embedding' in record_dict:
                    record_dict['embedding'] = self._convert_embedding(
                        record_dict['embedding'])
                data.append(Row(**record_dict))

            schema: StructType = BronzeRecord.model_spark_schema()
            data = [Row(**r.model_dump()) for r in records]

            df: DataFrame = self.spark.createDataFrame(
                data=data, schema=schema)

            df_partitioned: DataFrame = df.withColumn(
                colName="ingested_date",
                col=F.from_unixtime(F.col("ingested_at"), "yyyy-MM-dd")
            )
        except Exception as e:
            log.error("Error creating DataFrame", error=str(e))
            return 0

        log.info("Writing JSON to partitioned lakehouse", path=self.path)
        try:
            df_partitioned.write.mode("append").partitionBy(
                "ingested_date"
            ).json(self.path)
        except Exception as e:
            log.error("Error writing to lakehouse", error=str(e))
            return 0

        log.info("Records written to lakehouse")

        return len(records)
