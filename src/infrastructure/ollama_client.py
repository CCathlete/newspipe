# src/infrastructure/ollama_client.py

import httpx
from typing import Any
from dataclasses import dataclass, field
from structlog.typing import FilteringBoundLogger
from returns.result import Result, Success, Failure

from ..domain.models import BronzeTagResponse


@dataclass(slots=True, frozen=True)
class OllamaClient:
    model: str
    client: httpx.AsyncClient
    ollama_server_url: str
    embedding_model: str = "nomic-embed-text"

    logger: FilteringBoundLogger = field(init=False)

    @property
    def generate_url(self) -> str:
        return f"{self.ollama_server_url.rstrip('/')}/api/generate"

    @property
    def embedding_url(self) -> str:
        return f"{self.ollama_server_url.rstrip('/')}/api/embeddings"

    async def tag_chunk(
        self,
        source_url: str,  # The origin of the stream.
        content: str
    ) -> Result[BronzeTagResponse, Exception]:
        log = self.logger.bind(source_url=source_url)

        system_instruction: str = (
            "You are a geopolitical news classifier. Analyze the HTML chunk.\n"
            "Actions:\n"
            "'NEW_ARTICLE', 'CONTINUE', 'CLICKLINK', 'IRRELEVANT'.\n"
        )

        prompt: str = (
            f"{system_instruction}\n"
            f"Context source url: {source_url}\n"
            f"HTML Content: {content[:2000]}\n"
            "Output JSON only."
        )
        log.info("Tagging chunk", prompt=prompt)

        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0}
        }

        try:
            res = await self.client.post(
                self.generate_url,
                json=payload,
                timeout=30.0
            )
            res.raise_for_status()
            validated = BronzeTagResponse.model_validate_json(
                res.json().get("response", "{}"))
            log.info("Tagged chunk", validated=validated)
            return Success(validated)

        except Exception as e:
            log.error("Error tagging chunk", error=e)
            return Failure(e)

    async def embed_text(self, text: str) -> Result[list[float], Exception]:
        payload: dict[str, Any] = {
            "model": self.embedding_model,
            "prompt": text,
        }
        try:
            res = await self.client.post(self.embedding_url, json=payload, timeout=10.0)
            res.raise_for_status()
            return Success(res.json()["embedding"])
        except Exception as e:
            return Failure(e)
