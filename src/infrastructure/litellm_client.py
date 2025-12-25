# src/infrastructure/litellm_client.py

from httpx import AsyncClient, Response
from typing import Any
from dataclasses import dataclass
from structlog.typing import FilteringBoundLogger
from returns.result import Result, Success, Failure

from ..domain.models import BronzeTagResponse


@dataclass(slots=True, frozen=True)
class LitellmClient:
    model: str
    api_key: str
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

        prompt = (
            "You are a geopolitical news classifier. Analyze the HTML chunk.\n"
            "Return a valid JSON object matching this schema:\n"
            '{\n'
            f'  "chunk_id": "{chunk_id}",\n'
            f'  "source_url": "{source_url}",\n'
            f'  "content": "{content}",\n'
            '  "language": "string",\n'
            '  "control_action": "NEW_ARTICLE | CONTINUE | CLICKLINK | IRRELEVANT",\n'
            '  "reasoning": "string"\n'
            '}\n'
            'control_action MUST be one of the options provided in the schema.\n'
            'Output JSON only.'
        ).strip()

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "temperature": 0,
            "max_tokens": 500,
        }
        res: Response | None = None
        try:
            res = await self.client.post(
                url=self.chat_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=30.0
            )
            res.raise_for_status()
            raw = res.json()
            json_str = raw["choices"][0]["message"]["content"]
            validated = BronzeTagResponse.model_validate_json(json_str)
            log.info("Tagged chunk", validated=validated)
            return Success(validated)
        except Exception as e:
            assert res is not None, "Didn't get a response"
            body = await res.aread() if hasattr(res, "aread") else str(e)
            log.error(
                "Error tagging chunk",
                status_code=res.status_code,
                response_body=body,
                error=e,
            )
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
