# src/control/main.py

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Coroutine, Any
from domain.models import RelevancePolicy
from returns.result import Success, Failure, Result
from dependency_injector.wiring import inject, Provide
from dotenv import load_dotenv

from application.services.data_ingestion import IngestionPipeline
from domain.services.discovery_consumer import DiscoveryConsumer
from .dependency_layers import DataPlatformContainer

# Load environment variables
root_path: Path = Path(__file__).parents[2]
env_path: Path = root_path / ".env"
load_dotenv(env_path)


@inject
async def run_discovery(
    seeds_by_lang: dict[str, list[str]],
    pipeline: IngestionPipeline = Provide[DataPlatformContainer.pipeline],
    relevance_policy: RelevancePolicy = Provide[DataPlatformContainer.relevance_policy],
) -> None:
    """
    Orchestrator: Loads seed URLs and triggers the ingestion pipeline.
    Pushes discovered links to Kafka.
    """
    for lang, urls in seeds_by_lang.items():
        # Create a coroutine for every seed URL
        tasks: list[Coroutine[Any, Any, Result[int, Exception]]] = [
            pipeline.execute(start_url=url, language=lang, policy=relevance_policy) for url in urls
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


@inject
async def run_consumer_worker(
    consumer: DiscoveryConsumer = Provide[DataPlatformContainer.discovery_consumer],
) -> None:
    """
    Runs the DiscoveryConsumer to listen to the 'discovery_queue' topic
    and process links found during crawling.
    """
    try:
        await consumer.run()
    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    except Exception as e:
        print(f"Consumer crashed: {e}")
        raise


async def main_async() -> None:
    """
    Initializes the DI container, loads config, and starts the discovery process.
    """
    container = DataPlatformContainer()

    # Configuration
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
        "kafka": {
            "bootstrap_servers": "localhost:29092",
            "language_lookup": {}
        },
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
        # 1. Check for Seeds (Initial Burst)
        seed_path = root_path / "input_files" / "seed_urls.json"
        if seed_path.exists():
            with open(seed_path, "r") as f:
                seeds: dict[str, list[str]] = json.load(f)

            print("Running Seed Discovery Phase...")
            await run_discovery(seeds)
        else:
            print("No seed file found, skipping initial discovery.")

        # 2. Start the Consumer (Long-running)
        # This effectively switches the script to "Worker Mode"
        # It will block here until interrupted.
        print("Starting Discovery Consumer (Blocking)...")
        await run_consumer_worker()

    except Exception as e:
        print(f"Main crashed: {e}", file=sys.stderr)
        raise

    finally:
        # Clean shutdown
        if (shutdown_task := container.shutdown_resources()) is not None:
            await shutdown_task


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
