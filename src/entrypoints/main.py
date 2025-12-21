# src/entrypoints/main.py

from returns.result import Success, Failure, Result
from ..domain.services.data_ingestion import IngestionPipeline
from .dependency_layers import DataPlatformContainer
import asyncio
import json
import os
from pathlib import Path
from typing import Coroutine, Any
from dependency_injector.wiring import inject, Provide
from dotenv import load_dotenv
root_path: Path = Path(__file__).parents[2]
env_path: Path = root_path / ".env"
load_dotenv(env_path)


@inject
async def run_discovery(
    seeds_by_lang: dict[str, list[str]],
    pipeline: IngestionPipeline = Provide[DataPlatformContainer.pipeline]
) -> None:
    # Flattening the tasks while preserving language context
    for lang, urls in seeds_by_lang.items():

        tasks: list[Coroutine[Any, Any, Result[int, Exception]]] = [
            pipeline.execute(url, language=lang)
            for url in urls
        ]

        results: list[Result[int, Exception]] = await asyncio.gather(*tasks)

        for url, res in zip(urls, results):
            match res:
                case Success(count):
                    print(f"Seed Successful: {url} ({count} records)")

                case Failure(e):
                    print(f"Seed Failed: {url} | {e}")

                case _: pass


async def main_async() -> None:
    container = DataPlatformContainer()

    container.config.from_dict({
        "ollama": {"model": "llama3", "base_url": "http://localhost:11434"},
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
    })

    container.wire(modules=[__name__])

    if (init_task := container.init_resources()) is not None:
        await init_task

        # Everything happens inside the initialized context
        try:
            seed_path = root_path / "input_files" / "seed_urls.json"

            with open(seed_path, "r") as f:
                seeds: dict[str, list[str]] = json.load(f)

            await run_discovery(seeds)

        finally:
            if (shutdown_task := container.shutdown_resources()) is not None:
                await shutdown_task


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
