# src/domain/services/linguistic_model.py

import re
from dataclasses import dataclass, field
from returns.maybe import Some
from returns.result import Result, Success, Failure
from structlog.typing import FilteringBoundLogger

from ..interfaces import AIProvider
from ..models import BronzeRecord


@dataclass(slots=True, frozen=True)
class LinguisticService:
    ai_provider: AIProvider
    logger: FilteringBoundLogger = field(init=False)

    def tokenize(self, text: str) -> list[str]:
        return re.findall(r"\w+", text, re.UNICODE)

    def create_gm3(self, text: str) -> list[str]:
        tokens: list[str] = self.tokenize(text)
        if len(tokens) < 3:
            return []
        return [" ".join(tokens[i: i + 3]) for i in range(len(tokens) - 2)]

    async def generate_semantic_records(
        self,
        text: str,
        language: str,
        base_record: BronzeRecord
    ) -> Result[list[BronzeRecord], Exception]:
        grams: list[str] = self.create_gm3(text)
        results: list[BronzeRecord] = []
        log = self.logger.bind()

        for gram in grams:
            match await self.ai_provider.embed_text(gram):
                case Success(vector):
                    results.append(
                        BronzeRecord(
                            chunk_id=base_record.chunk_id,
                            source_url=base_record.source_url,
                            content=gram,
                            control_action="NONE",
                            language=language,
                            embedding=Some(vector),
                            gram_type="GM3"
                        )
                    )

                case Failure(_):
                    log.warning("embedding_failed", gram=gram)
                    continue

                case _: pass

        return Success(results)
