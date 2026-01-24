# src/infrastructure/litellm_client.py

from __future__ import annotations

import json
from typing import Any
from opentelemetry import trace
from dataclasses import dataclass
from httpx import AsyncClient, Response
from openinference.instrumentation import TracerProvider
from structlog.typing import FilteringBoundLogger
from returns.result import Result, Success, Failure


@dataclass(slots=True, frozen=True)
class LitellmClient:
    model: str
    api_key: str
    client: AsyncClient
    litellm_server_url: str
    logger: FilteringBoundLogger
    telemetry_observer: TracerProvider
    embedding_model: str = "nomic-embed-text"

    @property
    def _tracer(self) -> trace.Tracer:
        return trace.get_tracer(__name__, tracer_provider=self.telemetry_observer)

    @property
    def chat_url(self) -> str:
        return f"{self.litellm_server_url.rstrip('/')}/v1/chat/completions"

    @property
    def embed_url(self) -> str:
        return f"{self.litellm_server_url.rstrip('/')}/v1/embeddings"

    async def _post_request(
        self, 
        payload: dict[str, Any], 
        endpoint: str
    ) -> Result[Response, Exception]:
        # Define span name based on the action
        span_name = "chat_completion" if "chat" in endpoint else "embedding"
        
        # We use the OpenInference semantic conventions so Phoenix parses the data
        with self._tracer.start_as_current_span(span_name) as span:
            span.set_attribute("llm.model_name", self.model)
            span.set_attribute("llm.invocation_parameters", json.dumps(payload))
            
            # Record input for Phoenix UI
            if "messages" in payload:
                span.set_attribute("input.value", payload["messages"][0]["content"])
            elif "input" in payload:
                span.set_attribute("input.value", str(payload["input"]))

            try:
                response = await self.client.post(
                    url=endpoint,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                    timeout=60.0,
                )
                
                if response.status_code == 200:
                    res_json = response.json()
                    # Record output for Phoenix UI
                    if "choices" in res_json:
                        span.set_attribute("output.value", res_json["choices"][0]["message"]["content"])
                    
                    return Success(response)
                
                error_msg: str = f"LLM Error {response.status_code}: {response.text}"
                span.set_status(trace.Status(trace.StatusCode.ERROR, error_msg))
                return Failure(ValueError(error_msg))

            except Exception as e:
                span.record_exception(e)
                span.set_status(trace.Status(trace.StatusCode.ERROR))
                return Failure(e)

    def _extract_content(self, response_json: dict[str, Any]) -> Result[str, Exception]:
        match response_json:
            case {"choices": [{"message": {"content": str(content)}}, *_]}:
                return Success(content)
            case _:
                return Failure(ValueError(f"Invalid LLM response format: {response_json}"))

    async def is_relevant(
        self,
        text: str,
        policy_description: str,
        language: str = "en",
    ) -> Result[bool, Exception]:
        prompt = (
            f"SYSTEM: You are a strict relevance classifier.\n"
            f"POLICY: {policy_description}\n"
            f"LANGUAGE: {language}\n"
            f"TEXT: {text}\n"
            f"INSTRUCTION: Answer ONLY 'RELEVANT' or 'NOT RELEVANT'."
        )

        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
            "max_tokens": 5,
        }

        response = await self._post_request(payload, self.chat_url)
        
        return (
            response
            .bind(lambda res: Success(res.json()))
            .bind(self._extract_content)
            .map(lambda content: content.strip().upper())
            .map(lambda cleaned: "YES" in cleaned)
        )

    async def embed_text(self, text: str) -> Result[list[float], Exception]:
        payload = {"model": self.embedding_model, "input": text}
        response = await self._post_request(payload, self.embed_url)

        return (
            response
            .bind(lambda res: Success(res.json()))
            .bind(lambda j: Success(j["data"][0]["embedding"]))
        )
