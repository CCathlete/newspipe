# src/entrypoints/dependency_layers.py

import httpx
import structlog
from pyspark.sql import SparkSession
from aiokafka import AIOKafkaProducer
from dependency_injector import containers, providers
from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    cache_context,
    markdown_generation_strategy
)
from crawl4ai.chunking_strategy import OverlappingWindowChunking


from ..domain.services.data_ingestion import IngestionPipeline
from ..domain.services.linguistic_model import LinguisticService
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
        timeout=httpx.Timeout(60.0),
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
    )

    # Kafka Producer Resource
    kafka_producer = providers.Resource(
        AIOKafkaProducer,
        bootstrap_servers=config.kafka.bootstrap_servers
    )

    # Spark is usually provided as a singleton
    spark = providers.Singleton(
        SparkSession
        .Builder()
        .master("spark://localhost:7077")
        .appName("NewsAnalysis")
        .config("spark.hadoop.fs.s3a.endpoint", config.lakehouse.endpoint)
        # MinIO specific requirements
        .config("spark.hadoop.fs.s3a.access.key", config.lakehouse.username)
        .config("spark.hadoop.fs.s3a.secret.key", config.lakehouse.password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate
    )

    logger_provider = providers.Singleton(
        structlog.get_logger
    )

    browser_configuration = providers.Singleton(
        BrowserConfig,
        headless=True,
        # Standard high-reputation headers to avoid 401s
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        },
        enable_stealth=True  # Built-in Crawl4AI/Playwright stealth
    )

    # --- Infrastructure Layers ---

    strategy = providers.Singleton(
        OverlappingWindowChunking,
        window_size=config.streamScraper.window_size,
        overlap=config.stream_scraper.overlap,
    )

    run_config = providers.Singleton(
        CrawlerRunConfig,
        cache_mode=cache_context.CacheMode.BYPASS,
        chunking_strategy=strategy,
        markdown_generator=markdown_generation_strategy
        .DefaultMarkdownGenerator(
            options={"ignore_links": False}
        )
    )

    # Service Layers - analogous to ZLayer.live
    scraping_provider = providers.Factory(
        AsyncWebCrawler,
        config=browser_configuration,
    )

    scraper = providers.Factory(
        StreamScraper,
        crawler_factory=scraping_provider.provider,
        logger=logger_provider
    )

    ollama = providers.Factory(
        OllamaClient,
        model=config.ollama.model,
        ollama_server_url=config.ollama.base_url,
        client=http_client,
        logger=logger_provider
    )

    lakehouse = providers.Factory(
        LakehouseConnector,
        spark=spark,
        bucket_path=config.lakehouse.bronze_path,
        logger=logger_provider
    )

    # --- Domain Service Layers ---

    linguistic_service = providers.Factory(
        LinguisticService,
        ai_provider=ollama,
        language=config.app.default_language,
        logger=logger_provider
    )

    pipeline = providers.Factory(
        IngestionPipeline,
        scraper=scraper,
        ollama=ollama,
        lakehouse=lakehouse,
        kafka_producer=kafka_producer,
        logger=logger_provider
    )
