import httpx
from returns.result import Result, Success, Failure
from ..domain.models import BronzeTagResponse


class OllamaClient:
    def __init__(self, url: str, model: str, client: httpx.AsyncClient):
        self.url = f"{url.rstrip('/')}/api/generate"
        self.model = model
        self.client = client

    async def tag_chunk(self, article_id: str, content: str) -> Result[BronzeTagResponse, Exception]:
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
