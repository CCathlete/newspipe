# src/infrastructure/ollama_client.py

from dataclasses import dataclass, field
from structlog.typing import FilteringBoundLogger
import httpx
from returns.result import Result, Success, Failure
from ..domain.models import BronzeTagResponse


@dataclass(slots=True, frozen=True)
class OllamaClient:
    model: str
    client: httpx.AsyncClient
    base_url: str

    logger: FilteringBoundLogger = field(init=False)

    @property
    def url(self) -> str:
        return f"{self.base_url.rstrip('/')}/api/generate"

    async def tag_chunk(
            self,
            article_id: str,
            content: str
    ) -> Result[BronzeTagResponse, Exception]:

        log = self.logger.bind(article_id=article_id)
        log.info("ollama_tagging_started")

        prompt: str = f"ID: {article_id}\nContent: {content}\nOutput valid JSON with chunkId, articleId, controlAction."

        payload: dict[str, str | bool] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
        }

        try:
            res: httpx.Response = await self.client.post(self.url, json=payload)
            res.raise_for_status()

            # Extracting the 'response' field from Ollama's envelope
            raw_llm_text = res.json().get("response", "")
            if not raw_llm_text:
                log.warning(
                    f"Issue while extracting text from llm: {raw_llm_text}"
                )

            # Strict Pydantic validation (Runtime check)
            validated = BronzeTagResponse.model_validate_json(raw_llm_text)
            log.info("ollama_tagging_succeeded")
            return Success(validated)

        except Exception as e:
            log.error("ollama_tagging_failed", error=e)
            return Failure(e)
