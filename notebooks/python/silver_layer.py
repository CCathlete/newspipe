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

# %%
print(testval)


# %%
@safe
def initialize_platform() -> tuple[SparkSession, DataPlatformContainer, ConfigType]:
    config: ConfigType = {
        "lakehouse": {
            "bronze_path": "s3a://lakehouse/bronze",
            # "endpoint": "http://minio.catgineer.com",
            "endpoint": "http://localhost:9000",
            "username": os.getenv("MINIO_ACCESS_KEY", ""),
            "password": os.getenv("MINIO_SECRET_KEY", ""),
        },
        "spark_mode": "local[*]",
    }
    container: DataPlatformContainer = DataPlatformContainer()
    container.config.from_dict(config)

    return container.spark(), container, config

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
    df: DataFrame = spark.read.json(path)
    return df.withColumn(
        "source",
        F.regexp_extract(F.input_file_name(), r"bronze/([^/]+)/", 1)
    )

bronze_result: ResultE[DataFrame] = Failure(Exception("Initial value for bronze result."))

match setup_result:
    case Success((spark, container, config)):
        lakehouse_dict: dict[str, str] | str = config["lakehouse"]
        assert isinstance(lakehouse_dict, dict)
        bronze_path: str = lakehouse_dict["bronze_path"]
        bronze_result = load_bronze(spark, bronze_path)
        
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
