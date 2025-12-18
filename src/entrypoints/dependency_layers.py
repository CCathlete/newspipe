import httpx
from pyspark.sql import SparkSession
from dependency_injector import containers, providers

from ..infrastructure.scraper import StreamScraper
from ..infrastructure.ollama_client import OllamaClient
from ..infrastructure.lakehouse import LakehouseConnector


class DataPlatformContainer(containers.DeclarativeContainer):
    # Configuration - analogous to ZConfig
    config = providers.Configuration()

    # Resource Providers (Managed Lifecycles)
    # Like ZIO.acquireRelease for the HTTP Client
    http_client = providers.Resource(
        httpx.AsyncClient,
        timeout=httpx.Timeout(60.0)
    )

    # Spark is usually provided as a singleton
    spark = providers.Singleton(
        SparkSession.builder.appName("NewsAnalysis").getOrCreate
    )

    # Service Layers - analogous to ZLayer.live
    scraper = providers.Factory(
        StreamScraper,
        client=http_client
    )

    ollama = providers.Factory(
        OllamaClient,
        model=config.ollama.model,
        base_url=config.ollama.base_url,
        client=http_client
    )

    lakehouse = providers.Factory(
        LakehouseConnector,
        spark=spark,
        bucket_path=config.lakehouse.bronze_path
    )
