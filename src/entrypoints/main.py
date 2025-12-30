# src/entrypoints/main.py

import asyncio
import json
import os
from pathlib import Path
from typing import Coroutine, Any

from returns.result import Success, Failure, Result
from dependency_injector.wiring import inject, Provide
from dotenv import load_dotenv

from ..domain.services.data_ingestion import IngestionPipeline
from .dependency_layers import DataPlatformContainer

# Load environment variables
root_path: Path = Path(__file__).parents[2]
env_path: Path = root_path / ".env"
load_dotenv(env_path)


@inject
async def run_discovery(
    seeds_by_lang: dict[str, list[str]],
    pipeline: IngestionPipeline = Provide[DataPlatformContainer.pipeline],
) -> None:
    """
    Orchestrator: Loads seed URLs and triggers the ingestion pipeline.
    """
    for lang, urls in seeds_by_lang.items():
        # Create a coroutine for every seed URL
        tasks: list[Coroutine[Any, Any, Result[int, Exception]]] = [
            pipeline.execute(start_url=url, language=lang) for url in urls
        ]

        # Run concurrently
        results: list[Result[int, Exception]] = await asyncio.gather(*tasks)

        # Log results
        for url, res in zip(urls, results):
            match res:
                case Success(count):
                    print(f"Seed Successful: {url} ({count} records)")
                case Failure(e):
                    print(f"Seed Failed: {url} | {e}")
                case _:
                    pass


async def main_async() -> None:
    """
    Initializes the DI container, loads config, and starts the discovery process.
    """
    container = DataPlatformContainer()

    # Configuration (Ensure these keys match your Container's expected config)
    container.config.from_dict({
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
        "app": {"default_language": "en"},
        "kafka": {"bootstrap_servers": "localhost:29092"},
        "stream_scraper": {
            "window_size": 500,
            "overlap": 50,
        },
        "spark_mode": "local[*]" if os.getenv("LOCAL_SPARK_MODE") else "spark://localhost:7077",
    })

    # Wire modules for @inject to work
    container.wire(modules=[__name__])

    # Initialize resources (e.g., connect to Kafka, MinIO, Spark)
    if (init_task := container.init_resources()) is not None:
        await init_task

    try:
        seed_path = root_path / "input_files" / "seed_urls.json"
        if not seed_path.exists():
            print(f"Warning: Seed file not found at {seed_path}")
            return

        with open(seed_path, "r") as f:
            seeds: dict[str, list[str]] = json.load(f)

        await run_discovery(seeds)
    finally:
        # Clean shutdown
        if (shutdown_task := container.shutdown_resources()) is not None:
            await shutdown_task


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
