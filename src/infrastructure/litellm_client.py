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
    embedding_model: str = "nomic-embed-text" # UNUSED: Currently using openwebui built in embedding. 

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
                timeout=120.0,
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

        # Ultra-strict prompt with clear formatting requirements
        prompt = (
            "RETURN ONLY VALID JSON. NO OTHER TEXT. FORMAT:\n"
            '{"chunk_id":"' + chunk_id + '","source_url":"' + source_url + '",'
            '"content":"summary under 20 words.",'
            '"language":"en","control_action":"IRRELEVANT | NEW_ARTICLE | CLICKLINK | CONTINUE",'
            '"actions":[]}\n'
            "RULES:\n"
            f"- Data to summarise: {content}\n"
            "- NO reasoning, explanations, or extra text\n"
            "- Be decisive about geopolitical content\n"
            "- If geopolitical links exist, add to actions array:\n"
            '  {"url":"full_url","control_action":"CLICKLINK"}\n'
            "- If no geopolitical content: control_action=IRRELEVANT, actions=[]\n"
            "- OUTPUT ONLY JSON, NOTHING ELSE"
        ).strip()

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "temperature": 0.1,  # Very low for deterministic output
            "max_tokens": 500,
            "response_format": {"type": "json_object"}  # Force JSON response
        }

        def extract_content_from_response(
            response_json: dict[str, Any]
        ) -> Result[str, Exception]:
            """
            Attempts to extract the content from various LLM response formats.
            Tries multiple common patterns before giving up.
            """
            # Pattern 1: OpenAI-style with choices
            if "choices" in response_json:
                try:
                    return Success(response_json["choices"][0]["message"]["content"])
                except (KeyError, IndexError, TypeError) as e:
                    pass  # Try next pattern

            # Pattern 2: Anthropic-style with content blocks
            if "content" in response_json:
                try:
                    # Anthropic returns content as a list of blocks
                    content_blocks = response_json["content"]
                    if isinstance(content_blocks, list) and content_blocks:
                        return Success(content_blocks[0]["text"])
                except (KeyError, IndexError, TypeError) as e:
                    pass  # Try next pattern

            # Pattern 3: Cohere-style with text
            if "text" in response_json:
                try:
                    return Success(response_json["text"])
                except (KeyError, TypeError) as e:
                    pass  # Try next pattern

            # Pattern 4: Raw string response (some APIs return just a string)
            if isinstance(response_json, str):
                return Success(response_json)

            # Pattern 5: Direct content field
            if "content" in response_json:
                try:
                    content = response_json["content"]
                    if isinstance(content, str):
                        return Success(content)
                except (KeyError, TypeError) as e:
                    pass  # Try next pattern

            # If all patterns fail, convert the entire response to a string
            try:
                return Success(json.dumps(response_json))
            except Exception as e:
                return Failure(Exception(f"Could not extract content from response: {response_json}"))

        def extract_json_only(raw_content: str) -> Result[str, Exception]:
            """Extract only the JSON part, ignoring any surrounding text"""
            try:
                # Try to find JSON in the response
                start = raw_content.find('{')
                end = raw_content.rfind('}') + 1
                if start == -1 or end == 0:
                    return Failure(ValueError("No JSON found in response"))

                json_str = raw_content[start:end]

                # Remove any markdown code blocks
                json_str = json_str.replace(
                    '```json', '').replace('```', '').strip()

                return Success(json_str)
            except Exception as e:
                return Failure(e)

        def strict_parse_json(content: str) -> Result[dict[str, Any], Exception]:
            """Parse JSON with strict validation"""
            try:
                data = json.loads(content)

                # Remove any reasoning fields that might have snuck in
                if 'reasoning' in data:
                    del data['reasoning']
                if 'reasoning_content' in data:
                    del data['reasoning_content']
                if 'reasoning_details' in data:
                    del data['reasoning_details']

                return Success(data)
            except json.JSONDecodeError as e:
                return Failure(ValueError(f"Invalid JSON: {str(e)}"))
            except Exception as e:
                return Failure(e)

        def validate_and_normalize(
            data: dict[str, Any]
        ) -> Result[dict[str, Any], Exception]:
            """Validate and normalize the response to match BronzeTagResponse exactly"""
            try:

                # Validate metadata structure

                normalized = {
                    "chunkId": data.get("chunk_id", chunk_id),  # Using alias
                    "source_url": data.get("source_url", source_url),
                    # Using alias
                    "controlAction": data.get("control_action", "IRRELEVANT"),
                    "content": data.get("content", ""),
                    "language": data.get("language", "en"),
                    "actions": data.get("actions", []),
                }

                return Success(normalized)

            except Exception as e:
                return Failure(e)

        # Execute the request
        response_monad: Result[Response, Exception] = await self._post_request(payload)

        intermediate_result: Result[dict[str, Any], Exception] = (
            response_monad
            .bind(lambda res: Success(res.raise_for_status()))
            .bind(lambda res: Success(res.json()))
            .bind(extract_content_from_response)
            .bind(extract_json_only)
            .bind(strict_parse_json)
            .bind(validate_and_normalize)
        )

        result: Result[BronzeTagResponse, Exception] = (
            intermediate_result
            .bind(lambda data: Success(
                BronzeTagResponse.model_validate(data)
            ))
        )

        if isinstance(result, Failure):
            error = result.failure()
            log.error(
                "Error tagging chunk",
                error=str(error),
                chunk_id=chunk_id,
                source_url=source_url
            )
            # Fallback to a default response if parsing fails
            return Success(BronzeTagResponse(
                chunkId=chunk_id,
                source_url=source_url,
                controlAction="IRRELEVANT",
                content="Analysis failed",
                language="en",
                actions=[],
            ),
            )

        return result

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


    async def is_relevant(
        self,
        text: str,
        language: str,
        policy: dict[str, Any],
    ) -> Result[bool, Exception]:
        """
        Determine if the chunk is relevant given the policy.
        Returns Success(True) if relevant, Success(False) if not.
        """
        try:
            prompt = f"""
You are a relevance classifier.
Language: {language}
Policy: {json.dumps(policy)}
Text chunk: {text}

Answer only with YES if relevant, NO if not relevant.
"""
            # Call LiteLLM with a small completion / streaming
            llm_response: str = await self.complete(prompt)  # adjust to your existing async method
            llm_response = llm_response.strip().lower()
            if "yes" in llm_response:
                return Success(True)
            else:
                return Success(False)
        except Exception as e:
            return Failure(e)
