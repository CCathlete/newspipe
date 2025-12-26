# src/domain/models.py

from pydantic import BaseModel, Field, ConfigDict, field_serializer
from sparkdantic import SparkModel
from returns.maybe import Maybe, Nothing
from typing import Literal, TypeAlias
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
        embedding: Maybe[list[float]]
    ) -> list[float] | None:
        return embedding.unwrap() if embedding != Nothing else None
