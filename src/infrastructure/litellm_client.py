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

        # Initial prompt - more concise and strict
        initial_prompt = (
            "You are a geopolitical news classifier. Return ONLY a JSON object with this EXACT format:\n"
            '{\n'
            f'  "chunk_id": "{chunk_id}",\n'
            f'  "source_url": "{source_url}",\n'
            f'  "content": "summary in 20 words or less",\n'
            '  "language": "language code",\n'
            '  "control_action": "NEW_ARTICLE|CONTINUE|CLICKLINK|IRRELEVANT",\n'
            '  "actions": []\n'
            '}\n'
            'RULES:\n'
            '1. NO reasoning, explanations, or extra text\n'
            '2. Be decisive - if geopolitical, add to actions\n'
            '3. For links: add {"url": "full_url", "control_action": "CLICKLINK"}\n'
            '4. If no geopolitical content: "control_action": "IRRELEVANT"\n'
            '5. Output ONLY valid JSON, nothing else'
        ).strip()

        # Function to process a single iteration
        async def process_iteration(prompt: str) -> Result[dict[str, Any], Exception]:
            payload = {
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
                "temperature": 0,  # Lower for more deterministic results
                "max_tokens": 500,
                "response_format": {"type": "json_object"},  # Force JSON
                "top_p": 0.9,  # Nucleus sampling
                "frequency_penalty": 0.0,  # Don't penalize repetition
                "presence_penalty": 0.0,  # Don't penalize new tokens
            }

            response_monad = await self._post_request(payload)

            return (
                response_monad
                .bind(lambda res: Success(res.raise_for_status()))
                .bind(lambda res: Success(res.json()))
                .bind(lambda raw: Success(raw["choices"][0]["message"]["content"]))
                .bind(lambda content: Success(
                    content.strip()
                    .removeprefix("```json")
                    .removeprefix("```")
                    .strip("```")
                    .strip()
                ))
                .bind(lambda content: Success(json.loads(content)))
            )

        # First iteration
        first_result = await process_iteration(initial_prompt)

        if isinstance(first_result, Failure):
            return first_result

        first_data = first_result.unwrap()

        # Second iteration - refine the first result
        second_prompt = (
            "Refine this analysis to be more concise and accurate:\n"
            f"{json.dumps(first_data, indent=2)}\n"
            "Return ONLY the refined JSON object with the same format."
        ).strip()

        second_result = await process_iteration(second_prompt)

        if isinstance(second_result, Failure):
            return second_result

        second_data = second_result.unwrap()

        # Third iteration - final validation
        third_prompt = (
            "Final validation. Ensure this meets all requirements:\n"
            f"{json.dumps(second_data, indent=2)}\n"
            "Return ONLY the validated JSON object."
        ).strip()

        third_result = await process_iteration(third_prompt)

        if isinstance(third_result, Failure):
            return third_result

        final_data = third_result.unwrap()

        # Ensure we have all required fields
        try:
            normalized = {
                "chunk_id": final_data.get("chunk_id", chunk_id),
                "source_url": final_data.get("source_url", source_url),
                "content": final_data.get("content", ""),
                "language": final_data.get("language", "en"),
                "control_action": final_data.get("control_action", "IRRELEVANT"),
                "actions": final_data.get("actions", []),
            }

            # Validate the response
            if not normalized["content"]:
                return Failure(ValueError("Empty content in final response"))

            return Success(BronzeTagResponse.model_validate(normalized))
        except Exception as e:
            return Failure(e)

    async def embed_text(self, text: str) -> Result[list[float], Exception]:
        log = self.logger.bind()
        payload: dict[str, Any] = {
            "model": self.embedding_model,
            "input": text,
        }

        response_monad: Result[Response, Exception] = await self._post_request(payload)

        result: Result[list[float], Exception] = (
            response_monad.bind(
                lambda res: Success(res.raise_for_status()))
            .bind(lambda res: Success(res.json()))
            .bind(lambda j: Success(j["data"][0]["embedding"]))
        )

        if isinstance(result, Failure):
            log.error("Error tagging chunk", error=result.failure())

        return result
