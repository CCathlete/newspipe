# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
#     path: /usr/share/jupyter/kernels/python3
# ---

# %%
import os
import sys
from pathlib import Path
from typing import TypeAlias
from dotenv import load_dotenv

try:
    root_path: Path = Path(__file__).parents[2]
except NameError:
    root_path: Path = Path.cwd()

if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

load_dotenv(root_path / ".env")

from pyspark.sql import SparkSession, DataFrame
from src.control.dependency_layers import DataPlatformContainer
from pyspark.sql import functions as F
from returns.result import ResultE, Success, Failure, safe
import ipykernel
print(ipykernel.get_connection_file())


ConfigType: TypeAlias = dict[str, dict[str, str] | str]

testval: int = 8


def _resolve_and_validate_lakehouse_config(config: dict) -> dict[str, str]:
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

# %%
print(testval)


# %%
@safe
def initialize_platform() -> tuple[SparkSession, DataPlatformContainer, ConfigType]:
    config: ConfigType = {
        "lakehouse": {
            "bronze_path": "s3a://lakehouse/bronze/**",
            "endpoint": "http://localhost:9000",
            "username": os.getenv("MINIO_ACCESS_KEY", ""),
            "password": os.getenv("MINIO_SECRET_KEY", ""),
        },
        "spark_mode": "local[*]",
    }
    resolved_lakehouse_cfg_dict: dict[str, str] = _resolve_and_validate_lakehouse_config(config)

    container: DataPlatformContainer = DataPlatformContainer() # Future use.
    container.config.from_dict(config)

    spark: SparkSession = _create_spark_session(resolved_lakehouse_cfg_dict)

    return spark, container, config

setup_result: ResultE[tuple[SparkSession, DataPlatformContainer, ConfigType]] = initialize_platform()

spark: SparkSession

match setup_result:
    case Success((spark, container, config)):
        print("Spark Session Active")
        lake_config: dict[str, str] | str = config["lakehouse"]
    case Failure(err):
        print(f"Setup Failed: {err}")

# %%
@safe
def load_bronze(spark: SparkSession, path: str) -> DataFrame:
    df: DataFrame = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("mode", "PERMISSIVE")
    .json(path)
    )

    return df.withColumn(
        "source",
        F.regexp_extract(F.input_file_name(), r"bronze/([^/]+)/", 1)
    )


@safe
def inspect_prefix(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("binaryFile").load(path)


bronze_result: ResultE[DataFrame] = Failure(Exception("Initial value for bronze result."))

match setup_result:
    case Success((spark, container, config)):
        lakehouse_dict: dict[str, str] | str = config["lakehouse"]
        assert isinstance(lakehouse_dict, dict)
        bronze_path: str = lakehouse_dict["bronze_path"]

        sanity_test: ResultE[DataFrame] = inspect_prefix(spark, bronze_path)
        bronze_result: ResultE[DataFrame] = load_bronze(spark, bronze_path)
        
        match bronze_result:
            case Success(df):
                print("Bronze Data Loaded")
                df.printSchema()
                bronze_df: DataFrame = df
            case Failure(err):
                print(f"Load Failed: {err}")


# %%
@safe
def inspect_data(df: DataFrame) -> int:
    df.show(5, truncate=False)
    return df.count()

match bronze_result:
    case Success(df):
        inspection: ResultE[int] = inspect_data(df)
        match inspection:
            case Success(count):
                print(f"Total records in Bronze: {count}")
            case Failure(err):
                print(f"Inspection Error: {err}")
