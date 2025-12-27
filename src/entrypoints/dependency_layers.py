# src/entrypoints/dependency_layers.py

import sys
import httpx
import logging
import structlog
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from dependency_injector import containers, providers
from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    cache_context,
    markdown_generation_strategy
)
from crawl4ai.chunking_strategy import OverlappingWindowChunking
from structlog.processors import JSONRenderer, TimeStamper, StackInfoRenderer, format_exc_info


from ..domain.services.data_ingestion import IngestionPipeline
from ..domain.services.linguistic_model import LinguisticService
from ..infrastructure.scraper import StreamScraper
from ..infrastructure.litellm_client import LitellmClient
from ..infrastructure.lakehouse import LakehouseConnector

# ----------------------------------------------------------------------
# Configure root logging - JSON output to console and rotating file.
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)

# Ensure any previous file handler is removed
for h in logging.root.handlers[:]:
    if isinstance(h, (RotatingFileHandler, logging.FileHandler)):
        logging.root.removeHandler(h)

# RotatingFileHandler: truncates when it reaches maxBytes and overwrites
log_file_handler = RotatingFileHandler(
    filename="newspipe.log",
    maxBytes=150 * 1024,   # 150 KiB per file
    backupCount=1,               # keep only the latest file, discard older ones
    mode="w",                    # append to current file; rotation creates a new one
    encoding="utf-8",
)
log_file_handler.setLevel(logging.INFO)
log_file_handler.setFormatter(logging.Formatter("%(message)s"))
logging.root.addHandler(log_file_handler)


# ----------------------------------------------------------------------
# Structlog configuration - JSON output, timestamps, proper exc formatting.
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        TimeStamper(fmt='iso'),
        StackInfoRenderer(),
        format_exc_info,
        JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)


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

    kafka_consumer = providers.Resource(
        AIOKafkaConsumer,
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
        window_size=config.stream_scraper.window_size,
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
        kafka_consumer=kafka_consumer,
        logger=logger_provider
    )

    # ollama = providers.Factory(
    #     OllamaClient,
    #     model=config.ollama.model,
    #     ollama_server_url=config.ollama.base_url,
    #     client=http_client,
    #     logger=logger_provider
    # )

    litellm = providers.Factory(
        LitellmClient,
        model=config.litellm.model,
        api_key=config.litellm.api_key,
        litellm_server_url=config.litellm.base_url,
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
        # ai_provider=ollama,
        ai_provider=litellm,
        language=config.app.default_language,
        logger=logger_provider
    )

    pipeline = providers.Factory(
        IngestionPipeline,
        scraper=scraper,
        ollama=litellm,
        lakehouse=lakehouse,
        kafka_producer=kafka_producer,
        logger=logger_provider,
        strategy=strategy,
        run_config=run_config,
        # linguistic_service=linguistic_service
        linguistic_service=None
    )
