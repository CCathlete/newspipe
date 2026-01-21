# src/control/main.py

import asyncio
import json
import os
from pathlib import Path
from typing import Any
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
    seeds: dict[str, list[str]]
    to_include: list[str]
    to_exclude: list[str]

    if seeds_file_path.exists():
        with open(seeds_file_path, "r") as f:
            seeds_and_filters: dict[str, list[str]] = json.load(f)
            include_section: list[str] = seeds_and_filters.get("include", [])
            exclude_section: list[str] = seeds_and_filters.get("exclude", [])
            assert isinstance(include_section, list) and isinstance(exclude_section, list)
            to_include, to_exclude = include_section, exclude_section
            seeds = {
                language: urls for language, urls in seeds_and_filters.items() if language in to_include and language not in to_exclude
            }

    else:
        raise ValueError("No seed urls in %s", seeds_file_path)
    
    return seeds


async def run_discovery(
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

async def run_consumer_worker(consumer: DiscoveryConsumer) -> None:
    try:
        await consumer.run()
    except KeyboardInterrupt:
        print("Consumer stopped.")
    except Exception as e:
        print(f"Consumer crashed: {e}")
        raise

async def main_async() -> None:
    container: DataPlatformContainer = DataPlatformContainer()
    input_dir: Path = root_path / "input_files"
    
    traversal_cfg: dict[str, Any] = _get_active_config(input_dir / "traversal_policies.json")
    relevance_cfg: dict[str, Any] = _get_active_config(input_dir / "relevance_policies.json")
    seeds: dict[str, list[str]]  = _get_seed_urls(input_dir / "seed_urls.json")

    container.config.from_dict({
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
    })

    if (init_task := container.init_resources()) is not None:
        await init_task

    try:
        pipeline: IngestionPipeline = container.pipeline()
        relevance_policy: RelevancePolicy = container.relevance_policy()
        consumer: DiscoveryConsumer = container.discovery_consumer()

        results: tuple[BaseException | None, ...] = await asyncio.gather(
            run_discovery(seeds, pipeline, relevance_policy),
            run_consumer_worker(consumer),
            return_exceptions=True
        )
        if not all(result is None for result in results):
            raise ValueError("Process failed.")

    finally:
        if (shutdown_task := container.shutdown_resources()) is not None:
            await shutdown_task

def main() -> None:
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
