# src/control/main.py

import asyncio
import json
import os
from pathlib import Path
from threading import Thread
import signal # Added import
from typing import Any

import structlog
from dependency_injector.wiring import Provide, inject

from .dependency_layers import BasePlatformContainer
from .discovery_controller import DiscoveryController
from .ingestion_controller import IngestionController

root_path: Path = Path(__file__).parents[2]

def _get_active_config(file_path: Path) -> dict[str, Any]:
    with open(file_path, "r") as f:
        configs: list[dict[str, Any]] = json.load(f)
    active_configs: list[dict[str, Any]] = [c for c in configs if c.get("active") is True]
    if not active_configs:
        raise ValueError(f"No active configuration found in {file_path}")
    return active_configs[-1]

def _get_seed_urls(seeds_file_path: Path) -> dict[str, list[str]]:
    if not seeds_file_path.exists():
        raise ValueError(f"No seed urls in {seeds_file_path}")
    with open(seeds_file_path, "r") as f:
        data: dict[str, Any] = json.load(f)
    to_include: list[str] = data.get("include", [])
    to_exclude: list[str] = data.get("exclude", [])
    internal_keys: set[str] = {"include", "exclude"}
    return {
        lang: urls 
        for lang, urls in data.items() 
        if lang not in internal_keys 
        and lang in to_include 
        and lang not in to_exclude
    }

@inject
async def main_async(
    seeds: dict[str, list[str]],
    config: dict[str, Any],
    app_shutdown_event: asyncio.Event,
    logger: structlog.typing.FilteringBoundLogger = Provide[BasePlatformContainer.logger_provider],
) -> None:
    logger.debug(f"main_async(): app_shutdown_event ID (received): {id(app_shutdown_event)}")
    discovery_finished_event: asyncio.Event = asyncio.Event()


    discovery_controller_instance = DiscoveryController(seeds, config, logger, discovery_finished_event)
    ingestion_controller_instance = IngestionController(config, logger, app_shutdown_event) # Ingestion now listens to app_shutdown_event

    discovery_t: Thread = Thread(target=discovery_controller_instance)
    ingestion_t: Thread = Thread(target=ingestion_controller_instance)

    loop = asyncio.get_running_loop()
    def signal_handler():
        logger.info("Shutdown signal received. Initiating graceful application shutdown.")
        logger.debug(f"signal_handler(): app_shutdown_event ID: {id(app_shutdown_event)}")
        logger.debug(f"signal_handler(): IngestionController instance event ID: {id(ingestion_controller_instance.ingestion_shutdown_event)}")
        
        if ingestion_controller_instance.event_loop:
            ingestion_controller_instance.event_loop.call_soon_threadsafe(
                ingestion_controller_instance.ingestion_shutdown_event.set
            )
        
        app_shutdown_event.set() 
        
    loop.add_signal_handler(signal.SIGTERM, signal_handler)
    loop.add_signal_handler(signal.SIGINT, signal_handler)

    discovery_t.start()
    ingestion_t.start()

    done, pending = await asyncio.wait([
        asyncio.create_task(discovery_finished_event.wait()),
        asyncio.create_task(app_shutdown_event.wait())
    ], return_when=asyncio.FIRST_COMPLETED)

    if app_shutdown_event.is_set():
        logger.info("Application shutdown event triggered. Waiting for threads to join.")
    elif discovery_finished_event.is_set():
        logger.info("Discovery finished signal received. Waiting for ingestion to process remaining data and then shutdown via app_shutdown_event.")
        # If discovery finished, we want ingestion to continue until app_shutdown_event is set.
        # This implies that the application will run until explicitly shut down by a signal,
        # or some other mechanism not yet fully defined if ingestion is meant to stop on its own after discovery.
        # For now, it means ingestion will keep consuming until a signal.
        pass

    await loop.run_in_executor(None, discovery_t.join)
    await loop.run_in_executor(None, ingestion_t.join)

def main() -> None:
    input_dir: Path = root_path / "input_files"
    seeds: dict[str, list[str]] = _get_seed_urls(input_dir / "seed_urls.json")
    
    config: dict[str, Any] = {
        "policy": {
            "traversal": _get_active_config(input_dir / "traversal_policies.json"),
            "relevance": _get_active_config(input_dir / "relevance_policies.json")
        },
        "litellm": {
            "model": "openrouter/arcee-ai/trinity-large-preview:free",
            "base_url": "http://localhost:4000",
            "api_key": os.getenv("LITELLM_API_KEY"),
            "telemetry_endpoint": f"{os.getenv('PHOENIX_COLLECTOR_ENDPOINT')}/v1/traces",
            "telemetry_api_key": os.getenv("PHOENIX_API_KEY"), 
            "telemetry_project_name": "newspipe",
            "max_concurrency": 10
        },
        "lakehouse": {
            "bronze_path": "s3a://lakehouse/bronze",
            "endpoint": "http://localhost:9000",
            "username": os.getenv("MINIO_ACCESS_KEY"),
            "password": os.getenv("MINIO_SECRET_KEY"),
        },
        "kafka": {
            "bootstrap_servers": f"{os.getenv('HOST_IP')}:29092",
            "group_id": "newspipe_v1",
            "language_lookup": {}
        },
        "stream_scraper": {"window_size": 500, "overlap": 50},
        "spark_mode": "local[*]" if os.getenv("LOCAL_SPARK_MODE") else "spark://localhost:7077",
    }

    app_shutdown_event: asyncio.Event = asyncio.Event() # Create event here

    container: BasePlatformContainer = BasePlatformContainer()
    container.config.from_dict(config)
    container.wire(modules=[__name__])
    
    asyncio.run(main_async(seeds, config, app_shutdown_event))

if __name__ == "__main__":
    main()
