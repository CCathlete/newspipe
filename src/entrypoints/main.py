# src/entrypoints/main.py

import asyncio
import sys
from dependency_injector.wiring import inject, Provide

from .dependency_layers import DataPlatformContainer
from ..domain.services.data_ingestion import IngestionPipeline
from returns.result import Success, Failure


@inject
async def run_ingestion(
    url: str,
    pipeline: IngestionPipeline = Provide[DataPlatformContainer.pipeline]
) -> None:
    match await pipeline.execute(url):
        case Success(count):
            print(
                f"--- SUCCESS: Ingested {count} records (RAW + GM2 + GM3) ---")

        case Failure(e):
            print(f"--- FAILURE: {str(e)} ---")
            sys.exit(1)

        case _: pass


async def main():
    container = DataPlatformContainer()

    # 1. Configuration (Ideally from Environment/Terraform)
    container.config.ollama.model.from_value("llama3")
    container.config.ollama.base_url.from_value("http://localhost:11434")
    container.config.lakehouse.bronze_path.from_value("s3a://lakehouse/bronze")

    # New configs for our English/Slovak growth strategy
    container.config.app.default_language.from_value(
        "en")  # Switch to 'sk' for Slovak
    container.config.kafka.bootstrap_servers.from_value("localhost:9092")

    # 2. Wiring
    # We must wire the module where @inject is used
    container.wire(modules=[__name__])

    # 3. Resource Lifecycle
    # This triggers the AsyncClient and SparkSession creation
    if (init_task := container.init_resources()) is not None:
        await init_task
        test_url = "https://example-news-site.com/article-1"

        try:
            await run_ingestion(test_url)

        finally:
            if (shutdown_task := container.shutdown_resources()) is not None:
                await shutdown_task

if __name__ == "__main__":
    asyncio.run(main())
