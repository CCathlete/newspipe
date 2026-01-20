# src/control/dependency_layers.py

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

from domain.models import RelevancePolicy, TraversalRules
from application.services.data_ingestion import IngestionPipeline
from application.services.discovery_consumer import DiscoveryConsumer
from domain.services.scraper import StreamScraper
from infrastructure.litellm_client import LitellmClient
from infrastructure.lakehouse import LakehouseConnector

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
    endpoint = config['lakehouse']['endpoint']
    access_key = config['lakehouse']['username']
    secret_key = config['lakehouse']['password']
    bronze_path = config['lakehouse']['bronze_path']
    spark_mode = config['spark_mode']
    
    missing = []
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

def _create_spark_session(resolved_lakehouse_cfg_dict) -> SparkSession:
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
            # This tells Spark not to look for the "Magic" or "S3A" specific
            # committers that are failing to find their class.
            .config("spark.hadoop.fs.s3a.committer.name", "directory")
            .config("spark.sql.sources.commitProtocolClass",
                    "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
            # --- MinIO Specifics ---
            .config("spark.hadoop.fs.s3a.endpoint", resolved_lakehouse_cfg_dict['endpoint'])
            .config("spark.hadoop.fs.s3a.access.key", resolved_lakehouse_cfg_dict['access_key'])
            .config("spark.hadoop.fs.s3a.secret.key", resolved_lakehouse_cfg_dict['secret_key'])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            # --- S3A Retry/Timeout Configurations ---
            .config("spark.hadoop.fs.s3a.attempts.maximum", "5")
            .config("spark.hadoop.fs.s3a.retry.limit", "10")
            .config("spark.hadoop.fs.s3a.retry.interval", "5000")
            .config("spark.hadoop.fs.s3a.establish.timeout", "5000")
            .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
            # Memory config.
            .config("spark.driver.cores", "1")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
                )

class DataPlatformContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    logger_provider = providers.Singleton(structlog.get_logger)

    http_client = providers.Resource(
        httpx.AsyncClient,
        timeout=httpx.Timeout(60.0),
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.5",
        }
    )

    kafka_producer = providers.Resource(
        AIOKafkaProducer,
        bootstrap_servers=config.kafka.bootstrap_servers
    )

    kafka_consumer = providers.Resource(
        AIOKafkaConsumer,
        bootstrap_servers=config.kafka.bootstrap_servers
    )

    traversal_rules = providers.Factory(
        TraversalRules,
        allowed_domains=config.policy.allowed_domains,
        required_path_segments=config.policy.required_segments,
        blocked_path_segments=config.policy.blocked_segments,
        max_depth=config.policy.max_depth
    )

    relevance_policy = providers.Factory(
        RelevancePolicy,
        name=config.policy.name,
        description=config.policy.description,
        traversal=traversal_rules,
        include_terms=config.policy.include_terms,
        exclude_terms=config.policy.exclude_terms
    )

    resolved_lakehouse_config = providers.Factory(
        _resolve_and_validate_lakehouse_config,
        config=config,
        logger=logger_provider
    )

    
    spark = providers.Factory(
        _create_spark_session,
        resolved_lakehouse_cfg_dict=resolved_lakehouse_config
    )
    strategy = providers.Singleton(
        OverlappingWindowChunking,
        window_size=config.stream_scraper.window_size,
        overlap=config.stream_scraper.overlap,
    )

    browser_configuration = providers.Singleton(
        BrowserConfig,
        headless=True,
        enable_stealth=True
    )

    run_config = providers.Singleton(
        CrawlerRunConfig,
        cache_mode=cache_context.CacheMode.BYPASS,
        chunking_strategy=strategy,
        markdown_generator=markdown_generation_strategy.DefaultMarkdownGenerator(
            options={"ignore_links": False}
        )
    )

    scraping_provider = providers.Factory(
        AsyncWebCrawler,
        config=browser_configuration,
    )

    scraper = providers.Factory(
        StreamScraper,
        logger=logger_provider,
        kafka_provider=kafka_producer,
        crawler_factory=scraping_provider.provider,
        policy=relevance_policy
    )

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

    pipeline = providers.Factory(
        IngestionPipeline,
        scraper=scraper,
        llm_client=litellm,
        lakehouse=lakehouse,
        logger=logger_provider,
        policy=relevance_policy
    )

    discovery_consumer = providers.Factory(
        DiscoveryConsumer,
        kafka_provider=kafka_consumer,
        ingestion_pipeline=pipeline,
        logger=logger_provider,
        run_config=run_config,
        policy=relevance_policy,
        language_lookup=config.kafka.language_lookup
    )
