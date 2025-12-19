# src/domain/models.py

from pydantic import BaseModel, Field
from returns.maybe import Maybe, Nothing
from typing import Literal
import time


class BronzeTagResponse(BaseModel):
    chunk_id: str = Field(alias="chunkId")
    article_id: str = Field(alias="articleId")
    control_action: Literal[
        "IRRELEVANT",
        "NEW_ARTICLE",
        "CLICKLINK",
        "CONTINUE"
    ] = Field(alias="controlAction")
    metadata: Maybe[dict[str, str]] = Field(default=Nothing)


class BronzeRecord(BaseModel):
    chunk_id: str
    article_id: str
    content: str
    control_action: str
    ingested_at: float = Field(default_factory=time.time)
