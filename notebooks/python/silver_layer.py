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

ConfigType: TypeAlias = dict[str, dict[str, str] | str]

# %%
@safe
def initialize_platform() -> tuple[SparkSession, ConfigType]:
    config: ConfigType = {
        "lakehouse": {
            "bronze_path": "s3a://lakehouse/bronze",
            "endpoint": "http://minio.catgineer.com",
            "username": os.getenv("MINIO_ACCESS_KEY", ""),
            "password": os.getenv("MINIO_SECRET_KEY", ""),
        },
        "spark_mode": "local[*]",
    }
    container: DataPlatformContainer = DataPlatformContainer()
    container.config.from_dict(config)

    return container.spark(), config

setup_result: ResultE[tuple[SparkSession, ConfigType]] = initialize_platform()

match setup_result:
    case Success((spark, config)):
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

match setup_result:
    case Success((spark, config)):
        lakehouse_dict: dict[str, str] | str = config["lakehouse"]
        assert isinstance(lakehouse_dict, dict)
        bronze_path: str = lakehouse_dict["bronze_path"]
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

assert bronze_result
match bronze_result:
    case Success(df):
        inspection: ResultE[int] = inspect_data(df)
        match inspection:
            case Success(count):
                print(f"Total records in Bronze: {count}")
            case Failure(err):
                print(f"Inspection Error: {err}")
