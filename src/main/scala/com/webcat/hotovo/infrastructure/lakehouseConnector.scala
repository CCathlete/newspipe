package com.webcat.hotovo.infrastructure

import zio._
import com.webcat.hotovo.business.BronzeRecord

// --- ZIO Service Definition ---

/** Defines the service responsible for writing structured data batches to the
  * target data lake environment.
  */
trait LakehouseConnector {

  /** Writes a single batch of records to the specified layer (e.g., 'bronze').
    *
    * @param layer
    *   The destination layer (e.g., "bronze", "silver").
    * @param records
    *   The batch of records to write.
    * @return
    *   An effect indicating success or failure of the write operation.
    */
  def write(
      layer: String,
      records: Chunk[BronzeRecord]
  ): ZIO[Any, Throwable, Unit]
}

// --- Implementation (A Mock for now) ---

/** Concrete implementation that simulates writing to the lakehouse.
  */
class LakehouseConnectorLive extends LakehouseConnector {

  override def write(
      layer: String,
      records: Chunk[BronzeRecord]
  ): ZIO[Any, Throwable, Unit] = {
    // In a real application, this is where you would place S3/GCS/ADLS
    // or Delta/Iceberg specific write code.
    ZIO
      .logInfo(
        s"SINK: Successfully wrote ${records.size} records to the '$layer' layer."
      )
      .when(records.nonEmpty) // Only log if there are records to write
      .unlessZIO(
        ZIO.succeed(records.isEmpty)
      ) // ZIO equivalent of 'unless' if records.isEmpty is ZIO
      .unit // Ensure the return type is ZIO[Any, Throwable, Unit]
  }
}

// --- Companion Object and ZLayer Definition ---

object LakehouseConnector {

  /** Accessor method for the write operation. */
  def write(
      layer: String,
      records: Chunk[BronzeRecord]
  ): ZIO[LakehouseConnector, Throwable, Unit] =
    ZIO.serviceWithZIO[LakehouseConnector](_.write(layer, records))

  /** Live Layer construction: This service requires no dependencies itself
    * (ZLayer[Any, ...]) in its current mock state.
    */
  val live: ZLayer[Any, Nothing, LakehouseConnector] =
    ZLayer.succeed(new LakehouseConnectorLive)
}
