# src/infrastructure/lakehouse.py

from dataclasses import dataclass
from structlog.typing import FilteringBoundLogger
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, Row
from returns.result import safe
from returns.maybe import Maybe, Some, Nothing
from urllib.parse import urlparse
from typing import Iterable
from ..domain.models import BronzeRecord
from ..domain.interfaces import SparkSessionInterface


def _sanitize(text: str) -> str:
    return text.replace("/", "_").replace(":", "_").replace(".", "_")


def _source_identifier(url: str) -> str:
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
    def base_path(self) -> str:
        return self.bucket_path.rstrip("/") + "/"

    def _convert_embedding(self, embedding: Maybe[object]) -> Maybe[list[float]]:
        match embedding:
            case Some(v) if isinstance(v, list):
                return Some([float(x) for x in v])
            case _:
                return Nothing

    @safe
    def _to_row(self, rec: BronzeRecord) -> Maybe[Row]:
        try:
            d = rec.model_dump()
            emb = d.get("embedding")
            if emb is not None:
                d["embedding"] = self._convert_embedding(Some(emb)).unwrap()
            return Some(Row(**d))
        except Exception:
            return Nothing

    @safe
    def write_records(self, records: Iterable[BronzeRecord], source_url: str) -> int:
        log = self.logger.bind()
        if not records:
            log.warning("No records to write")
            return 0
        log.info("Writing records to lakehouse",
                 records_count=len(list(records)))
        rows: list[Row] = []
        for r in records:
            row_opt = self._to_row(r)
            match row_opt:
                case Some(row):
                    rows.append(row.unwrap())
                case Nothing:
                    continue
        if not rows:
            return 0
        schema: StructType = BronzeRecord.model_spark_schema()
        df: DataFrame = self.spark.createDataFrame(rows, schema)
        df_part = df.withColumn(
            "ingested_date",
            F.from_unixtime(F.col("ingested_at"), "yyyy-MM-dd")
        )
        identifier = _source_identifier(source_url)
        target_dir = f"{self.base_path}{identifier}/"
        df_part.write.mode("append").partitionBy(
            "ingested_date").json(target_dir)
        log.info("Records written to lakehouse", path=target_dir)
        return len(rows)
