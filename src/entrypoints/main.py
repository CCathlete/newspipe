# src/entrypoints/main.py

import asyncio
import json
from pathlib import Path
from dependency_injector.wiring import inject, Provide

from .dependency_layers import DataPlatformContainer
from ..domain.services.data_ingestion import IngestionPipeline
from returns.result import Success, Failure


@inject
async def run_discovery(
    urls: list[str],
    pipeline: IngestionPipeline = Provide[DataPlatformContainer.pipeline]
) -> None:
    tasks = [pipeline.execute(url) for url in urls]
    results = await asyncio.gather(*tasks)

    for url, res in zip(urls, results):
        match res:
            case Success(count):
                print(f"Seed Successful: {url} ({count} records)")

            case Failure(e):
                print(f"Seed Failed: {url} | {e}")

            case _: pass


async def main() -> None:
    container = DataPlatformContainer()

    container.config.from_dict({
        "ollama": {"model": "llama3", "base_url": "http://localhost:11434"},
        "lakehouse": {"bronze_path": "s3a://lakehouse/bronze"},
        "app": {"default_language": "en"},
        "kafka": {"bootstrap_servers": "localhost:9092"}
    })

    container.wire(modules=[__name__])

    if (init_task := container.init_resources()) is not None:
        await init_task

        # Everything happens inside the initialized context
        try:
            seed_path = Path(__file__).parents[2] / \
                "input_files" / "seed_urls.json"
            with open(seed_path, "r") as f:
                seeds: list[str] = json.load(f)

            await run_discovery(seeds)

        finally:
            if (shutdown_task := container.shutdown_resources()) is not None:
                await shutdown_task

if __name__ == "__main__":
    asyncio.run(main())
