# src/domain/models.py

from pydantic import BaseModel, Field, ConfigDict
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


class BronzeRecord(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    chunk_id: str
    source_url: str  # The origin of the stream.
    content: str
    control_action: str
    ingested_at: float = Field(default_factory=time.time)
    language: str = "sk"  # e.g., "sk", "he", "en", "ar"
    embedding: Maybe[list[float]] = Field(default=Nothing)

    gram_type: str = "GM3"
