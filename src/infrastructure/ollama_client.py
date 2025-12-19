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

        # Enhanced prompt for directional logic
        system_instruction: str = (
            "You are a geopolitical news classifier. Analyze the HTML chunk.\n"
            "Actions:\n"
            "1. 'NEW_ARTICLE': Chunk contains a headline/start of a relevant story.\n"
            "2. 'CONTINUE': Chunk is a continuation of the current story.\n"
            "3. 'CLICKLINK': Chunk contains a URL to a full article or related geopolitical event. "
            "Put the URL in metadata.url.\n"
            "4. 'IRRELEVANT': Chunk is navigation, ads, or unrelated news.\n"
        )

        prompt: str = (
            f"{system_instruction}\n"
            f"Context Article ID: {article_id}\n"
            f"HTML Content: {content[:2000]}\n"
            "Output JSON only."
        )

        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0}  # Deterministic for extraction
        }

        try:
            res: httpx.Response = await self.client.post(self.url, json=payload, timeout=30.0)
            res.raise_for_status()

            raw_response = res.json().get("response", "{}")

            # Pydantic handles the conversion of metadata dict to Maybe[dict]
            # based on our previous model definition.
            validated = BronzeTagResponse.model_validate_json(raw_response)

            log.info("ollama_tagging_succeeded",
                     action=validated.control_action)
            return Success(validated)

        except Exception as e:
            log.error("ollama_tagging_failed", error=str(e))
            return Failure(e)
