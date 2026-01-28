# %% 
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from dependency_injector.wiring import Provide, inject
from returns.result import Failure, Success

# Add project root to sys.path to allow importing from src
try:
    # Assumes the notebook is in notebooks/python/
    root_path = Path(__file__).parents[2]
except NameError:
    # __file__ is not defined in interactive mode, so use cwd
    root_path = Path.cwd()

if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

load_dotenv(root_path)

from pyspark.sql import SparkSession, DataFrame
from src.control.dependency_layers import DataPlatformContainer
from returns.io import IOResultE, IOSuccess, IOFailure, impure_safe

BRONZE_PATH = "s3a://lakehouse/bronze/tagged_chunks"

# %% 

@impure_safe
def get_spark_session() -> SparkSession:
    """
    Initializes the DataPlatformContainer and returns a configured SparkSession.
    This is an impure function as it depends on external configuration and environment variables.
    """
    config = {
        "lakehouse": {
            "bronze_path": "s3a://lakehouse/bronze",
            "endpoint": "http://localhost:9000",
            "username": os.getenv("MINIO_ACCESS_KEY"),
            "password": os.getenv("MINIO_SECRET_KEY"),
        },
        "spark_mode": "local[*]",
    }
    
    container = DataPlatformContainer()
    container.config.from_dict(config)
    
    # The provider will raise an exception if config is missing,
    # which will be caught by @impure_safe and returned as a IOFailure.
    spark = container.spark()
    return spark

@impure_safe
def read_bronze_dataframe(spark: SparkSession) -> DataFrame:
    """Reads the bronze data from the S3 data lake."""
    return spark.read.json(BRONZE_PATH)

@impure_safe
def show_dataframe(df: DataFrame) -> DataFrame:
    """Shows the DataFrame and returns it."""
    df.show()
    return df

# %% 
def main() -> None:
    """Main pipeline to connect to S3, read bronze data, and examine it."""
    
    session_result: IOResultE[SparkSession] = get_spark_session()

    match session_result:
        case IOSuccess(Success(spark)):
            print("Spark session created successfully.")
            
            bronze_result: IOResultE[DataFrame] = read_bronze_dataframe(spark)
            
            match bronze_result:
                case IOSuccess(Success(bronze_df)):
                    print("Successfully read from bronze layer. DataFrame schema:")
                    bronze_df.printSchema()
                    
                    print("\nBronze DataFrame content:")
                    show_result: IOResultE[DataFrame] = show_dataframe(bronze_df)
                    
                    match show_result:
                        case IOSuccess(Success(_)):
                            print("Successfully shown DataFrame.")
                        case IOFailure(Failure(err)):
                            print("Failed to show DataFrame: %s", err)
                case IOFailure(Failure(err)):
                    print("Failed to read from bronze layer: %s", err)
        case IOFailure(Failure(err)):
            print("Failed to create Spark session: %s", err)


if __name__ == "__main__":
    main()
