# src/domain/models.py

from pydantic import BaseModel, Field, ConfigDict, field_serializer
from sparkdantic import SparkModel
from returns.maybe import Maybe, Nothing
from typing import Literal
import time


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
    metadata: Maybe[dict[str, str]] = Field(default=Nothing)

    # For libraries that might not work with Nothing we can set an automatic
    # callback that would convert it to None.
    @field_serializer("metadata")
    def serialize_maybe_metadata(
        self,
        metadata: Maybe[dict[str, str]]
    ) -> dict[str, str] | None:
        return metadata.unwrap() if metadata != Nothing else None


class BronzeRecord(SparkModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    chunk_id: str
    source_url: str  # The origin of the stream.
    content: str
    control_action: str
    ingested_at: float = Field(default_factory=time.time)
    language: str = "sk"  # e.g., "sk", "he", "en", "ar"
    embedding: Maybe[list[float]] = Field(default=Nothing)
    metadata: Maybe[dict[str, str]] = Field(default=Nothing)

    gram_type: str = "GM3"

    # For libraries that might not work with Nothing we can set an automatic
    # callback that would convert it to None.
    @field_serializer("metadata")
    def serialize_maybe_metadata(
        self,
        metadata: Maybe[dict[str, str]]
    ) -> dict[str, str] | None:
        return metadata.unwrap() if metadata != Nothing else None

    @field_serializer("embedding")
    def serialize_maybe_embedding(
        self,
        embedding: Maybe[list[float]]
    ) -> list[float] | None:
        return embedding.unwrap() if embedding != Nothing else None
