# src/domain/models.py

from urllib.parse import ParseResult, urlparse
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
from typing import Literal, override
import time


class BronzeRecord(SparkModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    chunk_id: str
    source_url: str  # The origin of the stream.
    content: str
    ingested_at: float = Field(default_factory=time.time)
    language: str = "en"  # e.g., "sk", "he", "en", "ar"
    embedding: Maybe[list[float]] = Field(default=Nothing)


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
            StructField("ingested_at", DoubleType(), False),
            StructField("language", StringType(), False),
            StructField("embedding", ArrayType(FloatType()), True),  # Nullable
        ])


class TraversalRules(BaseModel):
    required_path_segments: list[str] = Field(default_factory=list)
    blocked_path_segments: list[str] = Field(default_factory=list)
    max_depth: int = 3

    def _path_segments(self, url: str) -> list[str]:
        parsed: ParseResult = urlparse(url)
        all_segments: list[str] = [
            seg.lower() for seg in parsed.path.split("/") if seg
        ]
        # without_base_path: list[str] = all_segments[5:]
        # return without_base_path
        return all_segments

    def is_path_allowed(self, url: str, current_depth: int) -> bool:
        if current_depth > self.max_depth:
            return False

        segments: list[str] = self._path_segments(url)

        # 1. Hard block
        if any(seg in segments for seg in self.blocked_path_segments):
            return False

        # Allow root + navigation levels
        if current_depth == 0:
            return True

        # 2. Required segments (exact match)
        if self.required_path_segments:
            return any(seg in segments for seg in self.required_path_segments)

        return True


class RelevancePolicy(BaseModel):
    name: str
    description: str
    include_terms: list[str] = Field(default_factory=list)
    exclude_terms: list[str] = Field(default_factory=list)

    def validate_content(self, content: str) -> bool:
        content_lower = content.lower()
        # Heavy-handed exclusion: if it contains 'html' or 'css', it's likely a UI leak
        if any(term.lower() in content_lower for term in self.exclude_terms):
            return False
        
        return any(term.lower() in content_lower for term in self.include_terms)



