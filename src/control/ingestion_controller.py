# src/control/ingestion_controller.py

import asyncio
from dataclasses import dataclass
from typing import Any, Optional

from dependency_injector.wiring import Provide, inject
from returns.io import IOResultE, IOSuccess, IOFailure
from returns.result import Success, Failure

from application.services.ingestion_service import IngestionService
from .dependency_layers import IngestionContainer

@dataclass
class IngestionController:
    config: dict[str, Any]
    logger: Any
    ingestion_shutdown_event: asyncio.Event
    event_loop: Optional[asyncio.AbstractEventLoop] = None # Added attribute

    def __post_init__(self):
        self.logger.debug(f"IngestionController.__post_init__(): ingestion_shutdown_event ID: {id(self.ingestion_shutdown_event)}")

    @inject
    async def _run_flow(
        self,
        service: IngestionService = Provide[IngestionContainer.ingestion_service]
    ) -> IOResultE[None]:
        return await service.run().awaitable()

    def __call__(self) -> None:
        self.logger.debug(f"IngestionController.__call__(): self.ingestion_shutdown_event ID: {id(self.ingestion_shutdown_event)}")
        container: IngestionContainer = IngestionContainer()
        container.config.from_dict(self.config)
        
        # This is the event from the main loop
        container.config.ingestion_shutdown_event.from_value(self.ingestion_shutdown_event)

        container.wire(modules=[__name__])
        
        # Create a new event loop for this thread
        self.event_loop = asyncio.new_event_loop() # Assign to self.event_loop
        asyncio.set_event_loop(self.event_loop)

        try:
            if (init_task := container.init_resources()) is not None:
                self.event_loop.run_until_complete(init_task)

            result: IOResultE[None] = self.event_loop.run_until_complete(self._run_flow())
            
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
                self.event_loop.run_until_complete(shutdown_task)
            self.event_loop.close()
