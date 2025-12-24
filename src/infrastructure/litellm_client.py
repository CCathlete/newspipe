# src/infrastructure/litellm_client.py

from httpx import AsyncClient
from typing import Any
from dataclasses import dataclass
from structlog.typing import FilteringBoundLogger
from returns.result import Result, Success, Failure

from ..domain.models import BronzeTagResponse


@dataclass(slots=True, frozen=True)
class LitellmClient:
    model: str
    client: AsyncClient
    litellm_server_url: str
    logger: FilteringBoundLogger
    embedding_model: str = "nomic-embed-text"

    @property
    def chat_url(self) -> str:
        return f"{self.litellm_server_url.rstrip('/')}/v1/chat/completions"

    @property
    def embed_url(self) -> str:
        return f"{self.litellm_server_url.rstrip('/')}/v1/embeddings"

    async def tag_chunk(self, chunk_id: str, source_url: str, content: str) -> Result[BronzeTagResponse, Exception]:
        log = self.logger.bind(source_url=source_url)
        prompt: str = f"""
        You are a geopolitical news classifier. Analyze the HTML chunk.
        Return a valid JSON object matching this schema:
        {{
            "chunk_id": "{chunk_id}",
            "source_url": "{source_url}",
            "content": "{content}",
            "language": "string",
            "control_action": "NEW_ARTICLE | CONTINUE | CLICKLINK | IRRELEVANT",
            "reasoning": "string"
        }}
        control_action MUST be one of the options provided in the schema.
        Output JSON only.
        """
        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "temperature": 0,
            "max_tokens": 500,
            "response_format": "application/json"
        }
        try:
            res = await self.client.post(self.chat_url, json=payload, timeout=30.0)
            res.raise_for_status()
            raw = res.json()
            json_str = raw["choices"][0]["message"]["content"]
            validated = BronzeTagResponse.model_validate_json(json_str)
            log.info("Tagged chunk", validated=validated)
            return Success(validated)
        except Exception as e:
            log.error("Error tagging chunk", error=e)
            return Failure(e)

    async def embed_text(self, text: str) -> Result[list[float], Exception]:
        payload: dict[str, Any] = {
            "model": self.embedding_model,
            "input": text,
        }
        try:
            res = await self.client.post(
                self.embed_url,
                json=payload,
                timeout=10.0
            )
            res.raise_for_status()
            return Success(res.json()["data"][0]["embedding"])
        except Exception as e:
            return Failure(e)
