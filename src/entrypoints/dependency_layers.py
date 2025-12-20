# src/entrypoints/dependency_layers.py

import httpx
import structlog
from pyspark.sql import SparkSession
from aiokafka import AIOKafkaProducer
from dependency_injector import containers, providers


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
        timeout=httpx.Timeout(60.0)
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

    # --- Infrastructure Layers ---

    # Service Layers - analogous to ZLayer.live
    scraper = providers.Factory(
        StreamScraper,
        client=http_client,
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
