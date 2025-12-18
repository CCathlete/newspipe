from dataclasses import dataclass
import httpx
from returns.result import Result, Success, Failure
from ..domain.models import BronzeTagResponse


@dataclass(slots=True)
class OllamaClient:
    model: str
    client: httpx.AsyncClient
    url: str

    def __post_init__(self):
        self.url: str = f"{self.url.rstrip('/')}/api/generate"

    async def tag_chunk(
            self,
            article_id: str,
            content: str
    ) -> Result[BronzeTagResponse, Exception]:

        prompt: str = f"ID: {article_id}\nContent: {content}\nOutput valid JSON with chunkId, articleId, controlAction."

        payload: dict[str, str | bool] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
        }

        try:
            res = await self.client.post(self.url, json=payload)
            res.raise_for_status()

            # Extracting the 'response' field from Ollama's envelope
            raw_llm_text = res.json().get("response", "")

            # Strict Pydantic validation (Runtime check)
            validated = BronzeTagResponse.model_validate_json(raw_llm_text)
            return Success(validated)
        except Exception as e:
            return Failure(e)
