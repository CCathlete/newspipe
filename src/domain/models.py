# src/domain/models.py

from pydantic import BaseModel, Field, ConfigDict, field_serializer
from sparkdantic import SparkModel
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    FloatType,
    ArrayType
)

from returns.maybe import Maybe, Nothing
from typing import Literal, TypeAlias, override
import time

actionsType: TypeAlias = list[dict[str, str | list[dict[str, str]] | str]]


class BronzeTagResponse(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    chunk_id: str = Field(alias="chunkId")
    source_url: str  # The origin of the stream.
    control_action: Literal[
        "IRRELEVANT",
        "NEW_ARTICLE",
        "CLICKLINK",
        "CONTINUE"
    ] = Field(alias="controlAction")
    language: str
    content: str
    actions: actionsType = Field(default=[])


class BronzeRecord(SparkModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    chunk_id: str
    source_url: str  # The origin of the stream.
    content: str
    control_action: str
    ingested_at: float = Field(default_factory=time.time)
    language: str = "sk"  # e.g., "sk", "he", "en", "ar"
    embedding: Maybe[list[float]] = Field(default=Nothing)

    gram_type: str = "GM3"

    @field_serializer("embedding")
    def serialize_maybe_embedding(
        self,
        embedding: Maybe[list[float]],
    ) -> list[float] | None:
        """Serialize Maybe embedding to a PySpark-compatible format"""
        if embedding == Nothing:
            return None

        try:
            value = embedding.unwrap()
            if value is None:
                return None
            # Ensure all elements are floats
            return [float(x) for x in value]
        except (TypeError, ValueError, AttributeError):
            return None

    @override
    @classmethod
    def model_spark_schema(
        cls,
        safe_casting: bool = True,
        by_alias: bool = False,
        mode: Literal['validation', 'serialization'] = 'validation',
        exclude_fields: bool = False
    ) -> StructType:
        """Generate Spark schema that properly handles nullable embeddings"""
        return StructType([
            StructField("chunk_id", StringType(), False),
            StructField("source_url", StringType(), False),
            StructField("content", StringType(), False),
            StructField("control_action", StringType(), False),
            StructField("ingested_at", DoubleType(), False),
            StructField("language", StringType(), False),
            StructField("embedding", ArrayType(FloatType()), True),  # Nullable
            StructField("gram_type", StringType(), False),
        ])


class RelevancePolicy(BaseModel):
    """Defines relevance criteria for filtering chunks before embedding."""
    name: str
    description: str
    include_terms: list[str] = Field(default_factory=list)
    exclude_terms: list[str] = Field(default_factory=list)
