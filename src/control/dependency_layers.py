# src/control/dependency_layers.py
import sys
import httpx
import logging
import structlog
from asyncio import Semaphore
from phoenix.otel import register
from pyspark.sql import SparkSession
from logging.handlers import RotatingFileHandler
from typing import Generator, Any, AsyncIterator
from openinference.instrumentation import TracerProvider

from dependency_injector import containers, providers
from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    # CrawlerRunConfig,
    # cache_context,
    # markdown_generation_strategy,
    AdaptiveCrawler,
    AdaptiveConfig,
)
from crawl4ai.chunking_strategy import OverlappingWindowChunking
from structlog.processors import JSONRenderer, TimeStamper, StackInfoRenderer, format_exc_info

from domain.models import RelevancePolicy, TraversalRules
from domain.services.data_ingestion import IngestionPipeline
from application.services.discovery_service import DiscoveryService
from application.services.ingestion_service import IngestionService
from domain.services.scraper import StreamScraper
from infrastructure.litellm_client import LitellmClient
from infrastructure.lakehouse import LakehouseConnector
from infrastructure.kafka import KafkaProducerAdapter, KafkaConsumerAdapter

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)

for h in logging.root.handlers[:]:
    if isinstance(h, (RotatingFileHandler, logging.FileHandler)):
        logging.root.removeHandler(h)

log_file_handler = RotatingFileHandler(
    filename="newspipe.log",
    maxBytes=150 * 1024,
    backupCount=1,
    mode="w",
    encoding="utf-8",
)
log_file_handler.setLevel(logging.INFO)
log_file_handler.setFormatter(logging.Formatter("%(message)s"))
logging.root.addHandler(log_file_handler)

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

def _resolve_and_validate_lakehouse_config(
    config: dict,
    logger: structlog.stdlib.BoundLogger,
) -> dict[str, str]:
    assert isinstance(config['lakehouse'], dict)
    endpoint: str = config['lakehouse']['endpoint']
    access_key: str = config['lakehouse']['username']
    secret_key: str = config['lakehouse']['password']
    bronze_path: str = config['lakehouse']['bronze_path']
    spark_mode: str = config['spark_mode']
    
    missing: list[str] = []
    if not endpoint: missing.append("lakehouse.endpoint")
    if not access_key: missing.append("lakehouse.username")
    if not secret_key: missing.append("lakehouse.password")
    if not bronze_path: missing.append("lakehouse.bronze_path")
    
    if missing:
        logger.error("Missing Lakehouse config", missing_keys=missing)
        raise ValueError(f"Missing keys: {missing}")
        
    return {
        "endpoint": endpoint,
        "access_key": access_key,
        "secret_key": secret_key,
        "bronze_path": bronze_path,
        "spark_mode": spark_mode
    }

def _create_spark_session(resolved_lakehouse_cfg_dict: dict[str, str]) -> SparkSession:
    builder: SparkSession.Builder = SparkSession.Builder()
    return (
        builder
        .master(resolved_lakehouse_cfg_dict['spark_mode'])
        .appName("NewsAnalysis")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.apache.hadoop:hadoop-common:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.apache.spark:spark-hadoop-cloud_2.12:3.5.1"
        )
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("spark.hadoop.fs.s3a.endpoint", resolved_lakehouse_cfg_dict['endpoint'])
        .config("spark.hadoop.fs.s3a.access.key", resolved_lakehouse_cfg_dict['access_key'])
        .config("spark.hadoop.fs.s3a.secret.key", resolved_lakehouse_cfg_dict['secret_key'])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "5")
        .config("spark.hadoop.fs.s3a.retry.limit", "10")
        .config("spark.hadoop.fs.s3a.retry.interval", "5000")
        .config("spark.hadoop.fs.s3a.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.driver.cores", "1")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

def setup_phoenix(endpoint: str, project: str, api_key: str) -> Generator[TracerProvider, None, None]:
    tracer_provider: TracerProvider = register(
        project_name=project,
        endpoint=endpoint,
        api_key=api_key
    )
    yield tracer_provider

async def init_kafka_producer(
    bootstrap_servers: str,
    logger: Any
) -> AsyncIterator[KafkaProducerAdapter]:
    adapter: KafkaProducerAdapter = KafkaProducerAdapter(
        bootstrap_servers=bootstrap_servers,
        logger=logger
    )
    await adapter._producer.start()
    yield adapter
    await adapter._producer.stop()

async def init_kafka_consumer(
    bootstrap_servers: str, 
    group_id: str, 
    topics: tuple[str, ...], 
    logger: Any
) -> AsyncIterator[KafkaConsumerAdapter]:
    adapter: KafkaConsumerAdapter = KafkaConsumerAdapter(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        topics=topics,
        logger=logger
    )
    await adapter._consumer.start()
    yield adapter
    await adapter._consumer.stop()

class BasePlatformContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    logger_provider = providers.Singleton(structlog.get_logger)

class DiscoveryContainer(BasePlatformContainer):
    kafka_producer = providers.Resource(
        init_kafka_producer,
        bootstrap_servers=BasePlatformContainer.config.kafka.bootstrap_servers,
        logger=BasePlatformContainer.logger_provider
    )

    kafka_consumer = providers.Resource(
        init_kafka_consumer,
        bootstrap_servers=BasePlatformContainer.config.kafka.bootstrap_servers,
        group_id=BasePlatformContainer.config.kafka.group_id,
        topics=providers.Callable(lambda: ("discovery_queue",)),
        logger=BasePlatformContainer.logger_provider
    )

    traversal_rules = providers.Factory(
        TraversalRules,
        required_path_segments=BasePlatformContainer.config.policy.traversal.required_path_segments,
        blocked_path_segments=BasePlatformContainer.config.policy.traversal.blocked_path_segments,
        max_depth=BasePlatformContainer.config.policy.traversal.max_depth
    )

    strategy = providers.Factory(
        OverlappingWindowChunking,
        window_size=BasePlatformContainer.config.stream_scraper.window_size,
        overlap=BasePlatformContainer.config.stream_scraper.overlap,
    )

    browser_configuration = providers.Factory(
        BrowserConfig,
        headless=True,
        browser_mode="builtin",
    )

    async_crawler = providers.Factory(
        AsyncWebCrawler,
        config=browser_configuration,
    )

    adaptive_config = providers.Factory(
        AdaptiveConfig,
        confidence_threshold=0.6,
        max_pages=500,
        top_k_links=10,
        min_gain_threshold=0.0,
        strategy="statistical",
    )

    scraping_provider = providers.Factory(
        AdaptiveCrawler,
        crawler=async_crawler,
        config=adaptive_config
    )

    scraper = providers.Factory(
        StreamScraper,
        logger=BasePlatformContainer.logger_provider,
        kafka_provider=kafka_producer,
        crawler=scraping_provider,
        traversal_rules=traversal_rules,
        strategy=strategy,
        query=BasePlatformContainer.config.policy.traversal.query,
    )

    discovery_semaphore = providers.Factory(
        Semaphore,
        BasePlatformContainer.config.litellm.max_concurrency
    )

    discovery_service = providers.Factory(
        DiscoveryService,
        scraper=scraper,
        kafka_consumer=kafka_consumer,
        visited_producer=kafka_producer,
        logger=BasePlatformContainer.logger_provider,
        semaphore=discovery_semaphore,
    )

class IngestionContainer(BasePlatformContainer):
    http_client = providers.Resource(
        httpx.AsyncClient,
        timeout=httpx.Timeout(60.0),
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.5",
        }
    )

    kafka_consumer = providers.Resource(
        init_kafka_consumer,
        bootstrap_servers=BasePlatformContainer.config.kafka.bootstrap_servers,
        group_id=BasePlatformContainer.config.kafka.group_id,
        topics=providers.Callable(lambda: ("relevant_chunks",)),
        logger=BasePlatformContainer.logger_provider
    )

    relevance_policy = providers.Factory(
        RelevancePolicy,
        name=BasePlatformContainer.config.policy.relevance.name,
        description=BasePlatformContainer.config.policy.relevance.description,
        include_terms=BasePlatformContainer.config.policy.relevance.include_terms,
        exclude_terms=BasePlatformContainer.config.policy.relevance.exclude_terms
    )

    resolved_lakehouse_config = providers.Factory(
        _resolve_and_validate_lakehouse_config,
        config=BasePlatformContainer.config,
        logger=BasePlatformContainer.logger_provider
    )

    spark = providers.Singleton(
        _create_spark_session,
        resolved_lakehouse_cfg_dict=resolved_lakehouse_config
    )

    telemetry = providers.Resource(
        setup_phoenix,
        endpoint=BasePlatformContainer.config.litellm.telemetry_endpoint,
        project=BasePlatformContainer.config.litellm.telemetry_project_name,
        api_key=BasePlatformContainer.config.litellm.telemetry_api_key,
    )

    ingestion_semaphore = providers.Factory(
        Semaphore,
        BasePlatformContainer.config.litellm.max_concurrency
    )

    litellm = providers.Factory(
        LitellmClient,
        model=BasePlatformContainer.config.litellm.model,
        api_key=BasePlatformContainer.config.litellm.api_key,
        litellm_server_url=BasePlatformContainer.config.litellm.base_url,
        client=http_client,
        logger=BasePlatformContainer.logger_provider,
        telemetry_observer=telemetry,
        semaphore=ingestion_semaphore,
    )

    lakehouse = providers.Factory(
        LakehouseConnector,
        spark=spark,
        bucket_path=BasePlatformContainer.config.lakehouse.bronze_path,
        logger=BasePlatformContainer.logger_provider
    )

    pipeline = providers.Factory(
        IngestionPipeline,
        llm=litellm,
        lakehouse=lakehouse,
        logger=BasePlatformContainer.logger_provider,
        buffer_size=5,
        relevance_policy=relevance_policy
    )

    ingestion_service = providers.Factory(
        IngestionService,
        ingestion_pipeline=pipeline,
        kafka_consumer=kafka_consumer,
        logger=BasePlatformContainer.logger_provider,
    )
