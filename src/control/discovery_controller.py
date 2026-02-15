import asyncio
from dataclasses import dataclass
from typing import Any

from dependency_injector.wiring import Provide, inject
from returns.io import IOResultE

from application.services.discovery_service import DiscoveryService
from .dependency_layers import DiscoveryContainer

@dataclass(frozen=True)
class DiscoveryController:
    seeds: dict[str, list[str]]
    config: dict[str, Any]
    logger: Any
    handle_result: Any

    @inject
    async def _run_flow(
        self,
        service: DiscoveryService = Provide[DiscoveryContainer.discovery_service]
    ) -> IOResultE[None]:
        return await service.run(self.seeds).awaitable()

    def __call__(self) -> None:
        container: DiscoveryContainer = DiscoveryContainer()
        container.config.from_dict(self.config)
        container.wire(modules=[__name__])
        
        loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            if (init_task := container.init_resources()) is not None:
                loop.run_until_complete(init_task)

            result: IOResultE[None] = loop.run_until_complete(self._run_flow())
            self.handle_result("Discovery", result, self.logger)
        finally:
            if (shutdown_task := container.shutdown_resources()) is not None:
                loop.run_until_complete(shutdown_task)
            loop.close()
