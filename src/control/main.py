# src/control/main.py

import asyncio
import json
import os
from pathlib import Path
from threading import Thread
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
    logger: structlog.typing.FilteringBoundLogger = Provide[BasePlatformContainer.logger_provider],
) -> None:
    discovery_t: Thread = Thread(target=DiscoveryController, args=(seeds, config, logger))
    ingestion_t: Thread = Thread(target=IngestionController, args=(config, logger))

    discovery_t.start()
    ingestion_t.start()

    while discovery_t.is_alive() or ingestion_t.is_alive():
        await asyncio.sleep(1)

    discovery_t.join()
    ingestion_t.join()

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

    container: BasePlatformContainer = BasePlatformContainer()
    container.config.from_dict(config)
    container.wire(modules=[__name__])
    
    asyncio.run(main_async(seeds, config))

if __name__ == "__main__":
    main()
