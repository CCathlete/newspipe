# src/control/main.py

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from returns.io import IOFailure, IOSuccess
from returns.result import Failure, Success
import structlog
from dependency_injector.wiring import Provide, inject

from application.services.discovery_consumer import DiscoveryConsumer
from .dependency_layers import DataPlatformContainer

root_path: Path = Path(__file__).parents[2]

def _get_active_config(file_path: Path) -> dict[str, Any]:
    with open(file_path, "r") as f:
        configs: list[dict[str, Any]] = json.load(f)
    
    active_configs = [c for c in configs if c.get("active") is True]
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
    internal_keys = {"include", "exclude"}

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
    discovery_service: DiscoveryConsumer = Provide[DataPlatformContainer.discovery_consumer],
    logger: structlog.typing.FilteringBoundLogger = Provide[DataPlatformContainer.logger_provider],
    container: DataPlatformContainer = Provide[DataPlatformContainer],
) -> None:

    if (init_task := container.init_resources()) is not None:
        await init_task

    logger.info("starting_discovery_application_service")

    match await discovery_service.run(seeds):
        case IOSuccess(Success(_)):
            logger.info("discovery_service_completed_gracefully")
        case IOFailure(Failure(e)):
            logger.error("discovery_service_crashed", error=str(e))

    if (shutdown_task := container.shutdown_resources()) is not None:
        await shutdown_task

def main() -> None:
    input_dir: Path = root_path / "input_files"
    seeds = _get_seed_urls(input_dir / "seed_urls.json")
    
    config: dict[str, Any] = {
        "policy": {
            "traversal": _get_active_config(input_dir / "traversal_policies.json"),
            "relevance": _get_active_config(input_dir / "relevance_policies.json")
        },
        "litellm": {
            "model": "openrouter/mistralai/devstral-2512:free",
            "base_url": "http://localhost:4000",
            "api_key": os.getenv("LITELLM_API_KEY"),
            "telemetry_endpoint": f"{os.getenv('PHOENIX_COLLECTOR_ENDPOINT')}/v1/traces",
            "telemetry_api_key": os.getenv("PHOENIX_API_KEY"), 
            "telemetry_project_name": "newspipe",
        },
        "lakehouse": {
            "bronze_path": "s3a://lakehouse/bronze",
            "endpoint": "http://localhost:9000",
            "username": os.getenv("MINIO_ACCESS_KEY"),
            "password": os.getenv("MINIO_SECRET_KEY"),
        },
        "kafka": {
            # "bootstrap_servers": "localhost:29092",
            "bootstrap_servers": f"{os.getenv("HOST_IP")}:29092",
            "language_lookup": {}
        },
        "stream_scraper": {"window_size": 500, "overlap": 50},
        "spark_mode": "local[*]" if os.getenv("LOCAL_SPARK_MODE") else "spark://localhost:7077",
    }

    container = DataPlatformContainer()
    container.config.from_dict(config)
    container.wire(modules=[__name__])
    
    asyncio.run(main_async(seeds))

if __name__ == "__main__":
    main()
