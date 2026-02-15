# src/control/ingestion_controller.py

import asyncio
from dataclasses import dataclass
from typing import Any

from dependency_injector.wiring import Provide, inject
from returns.io import IOResultE, IOSuccess, IOFailure
from returns.result import Success, Failure

from application.services.ingestion_service import IngestionService
from .dependency_layers import IngestionContainer

@dataclass(frozen=True)
class IngestionController:
    config: dict[str, Any]
    logger: Any

    @inject
    async def _run_flow(
        self,
        service: IngestionService = Provide[IngestionContainer.ingestion_service]
    ) -> IOResultE[None]:
        return await service.run().awaitable()

    def __call__(self) -> None:
        container: IngestionContainer = IngestionContainer()
        container.config.from_dict(self.config)
        container.wire(modules=[__name__])
        
        loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            if (init_task := container.init_resources()) is not None:
                loop.run_until_complete(init_task)

            result: IOResultE[None] = loop.run_until_complete(self._run_flow())
            
            match result:
                case IOSuccess(inner):
                    match inner:
                        case Success(_):
                            self.logger.info("Ingestion controller completed gracefully")
                        case Failure(e):
                            self.logger.error("Ingestion controller logic failure", error=str(e))
                case IOFailure(inner_failure):
                    match inner_failure:
                        case Failure(e):
                            self.logger.error("Ingestion controller IO crash", error=str(e))
        finally:
            if (shutdown_task := container.shutdown_resources()) is not None:
                loop.run_until_complete(shutdown_task)
            loop.close()
