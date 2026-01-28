# %% 
import os
import sys
from pathlib import Path
from typing import TypeAlias
from dotenv import load_dotenv

# Add project root to sys.path to allow importing from src
try:
    # Assumes the notebook is in notebooks/python/
    root_path = Path(__file__).parents[2]
except NameError:
    # __file__ is not defined in interactive mode, so use cwd
    root_path = Path.cwd()

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
def get_spark_session() -> tuple[SparkSession, ConfigType]:
    """
    Initializes the DataPlatformContainer and returns a configured SparkSession.
    This function is wrapped in `safe` to eagerly execute and capture any
    exceptions in a `Result` container.
    """
    config: ConfigType = {
        "lakehouse": {
            "bronze_path": "s3a://lakehouse/bronze",
            "endpoint": "http://localhost:9000",
            "username": os.getenv("MINIO_ACCESS_KEY", ""),
            "password": os.getenv("MINIO_SECRET_KEY", ""),
        },
        "spark_mode": "local[*]",
    }

    container = DataPlatformContainer()
    container.config.from_dict(config)

    # The provider will raise an exception if config is missing,
    # which will be caught by @safe and returned as a Failure.
    spark = container.spark()
    return spark, config

@safe
def read_bronze_dataframe(spark: SparkSession, bronze_path: str) -> DataFrame:
    """
    Reads the partitioned bronze data from all sources in the S3 data lake,
    adding a `source` column extracted from the file path.
    """
    # Read all JSON files recursively. Spark will discover partitions (ingested_date).
    read_path = os.path.join(bronze_path, "*", "ingested_date=*")
    df = spark.read.json(read_path)

    # The source name is part of the file path. We extract it into a 'source' column.
    # The path looks like: s3a://.../bronze/<source>/ingested_date=.../file.json
    df_with_source = df.withColumn(
        "source",
        F.regexp_extract(F.input_file_name(), r"bronze\/(.*?)\/ingested_date=", 1)
    )
    
    return df_with_source

@safe
def show_dataframe(df: DataFrame) -> DataFrame:
    """Shows the DataFrame and returns it."""
    df.show()
    return df

# %% 
def main() -> None:
    """Main pipeline to connect to S3, read bronze data, and examine it."""

    result_tuple: ResultE[tuple[SparkSession, ConfigType]] = get_spark_session()

    match result_tuple:
        case Success((spark, config_dict)):
            print("Spark session created successfully.")
            
            assert isinstance(config_dict["lakehouse"], dict)
            bronze_result: ResultE[DataFrame] = read_bronze_dataframe(
                spark=spark,
                bronze_path=config_dict["lakehouse"]["bronze_path"]
            )
            
            match bronze_result:
                case Success(bronze_df):
                    print("Successfully read from bronze layer. DataFrame schema:")
                    bronze_df.printSchema()
                    
                    print("\nBronze DataFrame content:")
                    show_result: ResultE[DataFrame] = show_dataframe(bronze_df)
                    
                    match show_result:
                        case Success(_):
                            print("Successfully shown DataFrame.")
                        case Failure(err):
                            print(f"Failed to show DataFrame: {err}")
                case Failure(err):
                    print(f"Failed to read from bronze layer: {err}")
        case Failure(err):
            print(f"Failed to create Spark session: {err}")


if __name__ == "__main__":
    main()
