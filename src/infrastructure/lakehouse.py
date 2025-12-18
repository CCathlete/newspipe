from pyspark.sql import SparkSession, DataFrame
from returns.result import safe
from typing import cast, Any
from ..domain.models import BronzeRecord


class LakehouseConnector:
    def __init__(
        self, spark: SparkSession,
        bucket_path: str = "s3a://lakehouse/bronze/tagged_chunks",
    ) -> None:

        self.spark = spark
        self.path = bucket_path.rstrip("/") + "/"

    @safe
    def write_records(
        self, records: list[BronzeRecord],
    ) -> int:

        if not records:
            return 0

        # Convert Pydantic models to dictionaries for Spark
        data: list[dict[str, str]] = [r.model_dump() for r in records]

        df: DataFrame = self.spark.createDataFrame(
            cast(Any, data),
        )

        # Partitioning by article_id for optimized geo-political analysis later
        df.write.mode("append").partitionBy(
            "article_id"
        ) .json(self.path)

        return len(records)
