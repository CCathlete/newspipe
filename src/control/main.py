# src/control/main.py

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any
from dependency_injector.wiring import Provide
from returns.result import Success, Failure, Result

from application.services.data_ingestion import IngestionPipeline
from domain.services.discovery_consumer import DiscoveryConsumer
from domain.models import RelevancePolicy
from .dependency_layers import DataPlatformContainer

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


async def run_ingestion(
    seeds_by_lang: dict[str, list[str]],
    pipeline: IngestionPipeline,
    relevance_policy: RelevancePolicy,
) -> None:
    for lang, urls in seeds_by_lang.items():
        tasks = [
            pipeline.execute(
                start_url=url, 
                language=lang, 
                policy=relevance_policy
            ) for url in urls
        ]
        results: list[Result[int, Exception]] = await asyncio.gather(*tasks)
        
        for url, res in zip(urls, results):
            match res:
                case Success(count):
                    print(f"Seed Successful: {url} ({count} records)")
                case Failure(e):
                    print(f"Seed Failed: {url} | {e}")

async def run_stream_consumer(consumer: DiscoveryConsumer) -> None:
    try:
        await consumer.run()
    except KeyboardInterrupt:
        print("Consumer stopped.")
    except Exception as e:
        print(f"Consumer crashed: {e}")
        raise

async def main_async(
    seeds: dict[str, list[str]],
    pipeline: IngestionPipeline = Provide[DataPlatformContainer.pipeline],
    relevance_policy: RelevancePolicy = Provide[DataPlatformContainer.relevance_policy],
    consumer: DiscoveryConsumer = Provide[DataPlatformContainer.discovery_consumer],
    logger: logging.Logger = Provide[DataPlatformContainer.logger_provider],
    container: DataPlatformContainer = Provide[DataPlatformContainer],
) -> None:

    if (init_task := container.init_resources()) is not None:
        await init_task

    try:
        results: tuple[BaseException | None, ...] = await asyncio.gather(
            run_ingestion(seeds, pipeline, relevance_policy),
            run_stream_consumer(consumer),
            return_exceptions=True
        )
        if not all(result is None for result in results):
            logger.info("Results are all None: %s", results)
    except Exception as e:
        logger.error("Process failed: %s", e)
        

    finally:
        if (shutdown_task := container.shutdown_resources()) is not None:
            await shutdown_task

def main() -> None:
    input_dir: Path = root_path / "input_files"
    seeds: dict[str, list[str]]  = _get_seed_urls(input_dir / "seed_urls.json")
    
    traversal_cfg: dict[str, Any] = _get_active_config(input_dir / "traversal_policies.json")
    relevance_cfg: dict[str, Any] = _get_active_config(input_dir / "relevance_policies.json")

    config: dict[str, Any] = {
        "policy": {
            "traversal": traversal_cfg,
            "relevance": relevance_cfg
        },
        "litellm": {
            "model": "openrouter/mistralai/devstral-2512:free",
            "base_url": "http://localhost:4000",
            "api_key": os.getenv("LITELLM_API_KEY")
        },
        "lakehouse": {
            "bronze_path": "s3a://lakehouse/bronze",
            "endpoint": "http://localhost:9000",
            "username": os.getenv("MINIO_ACCESS_KEY"),
            "password": os.getenv("MINIO_SECRET_KEY"),
        },
        "kafka": {
            "bootstrap_servers": "localhost:29092",
            "language_lookup": {}
        },
        "stream_scraper": {"window_size": 500, "overlap": 50},
        "spark_mode": "local[*]" if os.getenv("LOCAL_SPARK_MODE") else "spark://localhost:7077",
    }

    container: DataPlatformContainer = DataPlatformContainer()
    container.config.from_dict(config)
    container.wire(modules=[__name__])
    asyncio.run(main_async(seeds))

if __name__ == "__main__":
    main()
