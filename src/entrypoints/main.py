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
    # Our bound logger is already inside 'pipeline', but we can log here too
    print(f"--- Starting Ingestion for: {url} ---")

    result = await pipeline.execute(url)

    match result:
        case Success(count):
            print(f"Successfully ingested {count} records into the Lakehouse.")
        case Failure(e):
            print(f"Pipeline failed: {str(e)}")
            sys.exit(1)


async def main():
    container = DataPlatformContainer()

    # 1. Map config values (In prod, these come from environment variables)
    container.config.ollama.model.from_value("llama3")
    container.config.ollama.base_url.from_value("http://localhost:11434")
    container.config.lakehouse.bronze_path.from_value("s3a://lakehouse/bronze")

    # 2. Wire the container to the modules using @inject
    # We wire this module AND the domain pipeline
    container.wire(modules=[__name__])

    # 3. Initialize Managed Resources (httpx client, Spark session)
    await container.init_resources()

    test_url = "https://example-news-site.com/article-1"

    try:
        await run_ingestion(test_url)
    finally:
        # 4. Graceful shutdown (Close Spark and HTTP Client)
        await container.shutdown_resources()

if __name__ == "__main__":
    asyncio.run(main())
