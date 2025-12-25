# src/infrastructure/litellm_client.py
from __future__ import annotations

import json
from typing import Any
from dataclasses import dataclass
from httpx import AsyncClient, Response
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

    async def _post_request(
        self,
        payload: dict[str, Any]
    ) -> Result[Response, Exception]:

        try:
            response = await self.client.post(
                url=self.chat_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=30.0,
            )
            return Success(response)
        except Exception as e:
            return Failure(e)

    async def tag_chunk(
        self,
        chunk_id: str,
        source_url: str,
        content: str,
    ) -> Result[BronzeTagResponse, Exception]:
        log = self.logger.bind(source_url=source_url)

        prompt = (
            "You are a geopolitical news classifier. Analyse the HTML chunk "
            "and return a valid JSON object that contains:\n"
            '{\n'
            f'  "chunk_id": "{chunk_id}",\n'
            f'  "source_url": "{source_url}",\n'
            f'  "content": "{content}"\n'
            '  "language": "string",\n'
            '  "control_action": "NEW_ARTICLE | CONTINUE | CLICKLINK | IRRELEVANT",\n'
            '  "reasoning": "string"\n'
            '}\n'
            "Only output the JSON object, no extra text.\n"
            "For every hyperlink that points to a geopolitically relevant "
            "destination, add a separate entry in the \"actions\" array:\n"
            '{\n'
            '  "url": "<hyperlink-url>",\n'
            '  "control_action": "CLICKLINK",\n'
            '  "reasoning": "link points to a geopolitical story"\n'
            "}\n"
            "If no geopolitical link is found, keep \"control_action\" as "
            "'IRRELEVANT' and omit the \"actions\" array.\n"
            "The top-level object must still contain the original fields "
            "(chunk_id, source_url, content, language, control_action, reasoning)."
        ).strip()

        log.info("Tagging chunk", prompt=prompt)

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "temperature": 0,
            "max_tokens": 800,
        }

        def clean_response_content(raw_content: str) -> Result[str, Exception]:
            try:
                cleaned: str = (
                    raw_content.strip()
                    .removeprefix("```json")
                    .removeprefix("```")
                    .strip("```")
                    .strip()
                )

                if not cleaned:
                    return Failure(ValueError("Empty response content"))
                return Success(cleaned)
            except Exception as e:
                return Failure(e)

        def parse_json_response(
            content: str
        ) -> Result[dict[str, Any], Exception]:
            try:
                if not content:
                    return Failure(ValueError("Empty JSON content"))
                return Success(json.loads(content))
            except json.JSONDecodeError as e:
                return Failure(ValueError(f"Invalid JSON: {str(e)}"))
            except Exception as e:
                return Failure(e)

        def normalize_response(
            parsed: dict[str, Any] | list[Any]
        ) -> Result[dict[str, Any], Exception]:
            if isinstance(parsed, list):
                top = parsed[0]
                actions = parsed[1:] if len(parsed) > 1 else []
            else:
                top = parsed
                actions = []

            if not isinstance(top, dict):
                return Failure(ValueError("Response is not a dictionary"))

            try:

                normalised: dict[str, Any] = {
                    "chunk_id": top.get("chunk_id", chunk_id),
                    "source_url": top.get("source_url", source_url),
                    "content": top.get("content", ""),
                    "language": top.get("language", "en"),
                    "control_action": top.get("control_action", "IRRELEVANT"),
                    "reasoning": (
                        top.get("reasoning", "")
                        + (
                            "\n\nDetected click actions:\n"
                            + "\n".join(
                                json.dumps(
                                    a, ensure_ascii=False)
                                for a in actions)
                            if actions
                            else ""
                        )
                    ),
                }

                if isinstance(result, Failure):
                    error = result.failure()
                    log.error(
                        "Error tagging chunk",
                        error=str(error),
                        error_type=type(error).__name__,
                        chunk_id=chunk_id,
                        source_url=source_url
                    )

                return Success(normalised)

            except Exception as e:
                return Failure(e)

        # Monadic composition using explicit bind calls
        response_monad: Result[Response, Exception] = await self._post_request(payload)

        result: Result[BronzeTagResponse, Exception] = (
            response_monad.bind(lambda res: Success(res.raise_for_status()))
            .bind(lambda res: Success(res.json()))
            .bind(lambda raw: Success(raw["choices"][0]["message"]["content"]))
            .bind(clean_response_content)
            .bind(parse_json_response)
            .bind(normalize_response)
            .bind(
                lambda enriched: Success(
                    json.dumps(
                        enriched,
                        ensure_ascii=False
                    )))
            .bind(
                lambda json_str: Success(
                    BronzeTagResponse.model_validate_json(json_str)
                ))
        )

        if isinstance(result, Failure):
            log.error("Error tagging chunk", error=result.failure())

        return result

    async def embed_text(self, text: str) -> Result[list[float], Exception]:
        log = self.logger.bind()
        payload: dict[str, Any] = {
            "model": self.embedding_model,
            "input": text,
        }

        response_monad: Result[Response, Exception] = await self._post_request(payload)

        result: Result[list[float], Exception] = (
            response_monad.bind(lambda res: Success(res.raise_for_status()))
            .bind(lambda res: Success(res.json()))
            .bind(lambda j: Success(j["data"][0]["embedding"]))
        )

        if isinstance(result, Failure):
            log.error("Error tagging chunk", error=result.failure())

        return result
