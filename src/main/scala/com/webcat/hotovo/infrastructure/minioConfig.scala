package com.webcat.hotovo.infrastructure

import zio._
import zio.config._
import zio.Config._
import zio.ZLayer
import zio.ConfigProvider

/** Configuration for the MinIO/S3 connection details.
  */
case class MinIOConfig(
    endpoint: String,
    bucket: String,
    accessKey: String,
    secretKey: String
)

object MinIOConfig {

  // Define the blueprint for loading configuration from the environment
  private val minioConfigSpec: Config[MinIOConfig] =
    (
      string("MINIO_ENDPOINT") ?? "MinIO/S3 server URL" zip
        string("MINIO_BUCKET") ?? "Target bucket for data lake" zip
        string("MINIO_ACCESS_KEY") ?? "Access key ID" zip
        string("MINIO_SECRET_KEY") ?? "Secret access key"
    ).to[MinIOConfig]

  // Layer to load the configuration
  val live: ZLayer[Any, Config.Error, MinIOConfig] = ZLayer.fromZIO(
    ConfigProvider.defaultProvider
      .load(minioConfigSpec)
      .tapError(e =>
        zio.ZIO
          .logError(s"Configuration error loading MinIOConfig: ${e.getMessage}")
      )
  )
}
