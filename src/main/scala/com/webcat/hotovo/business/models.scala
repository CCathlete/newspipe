package com.webcat.hotovo.business

import io.circe._
import io.circe.generic.semiauto._

// --- 1. Ollama Tagging Models (Used by OllamaClient) ---

/** The request payload sent to the Ollama service for tagging a single chunk.
  */
case class TagBronzeChunkRequest(
    currentArticleId: String,
    htmlChunk: String
)

object TagBronzeChunkRequest {
  // Circe Encoder for serialization
  implicit val encoder: Encoder[TagBronzeChunkRequest] =
    deriveEncoder
}

/** The structured response received from the Ollama service. Used by the
  * PipelineService to manage state and filtering.
  */
case class TagBronzeChunkResponse(
    chunkId: String,
    articleId: String,
    controlAction: String // "CONTINUE", "NEW_ARTICLE", "IRRELEVANT"
)

object TagBronzeChunkResponse {
  // Circe Decoder for deserialization
  implicit val decoder: Decoder[TagBronzeChunkResponse] =
    deriveDecoder
}

// --- 2. Lakehouse Data Model (Used by PipelineService and LakehouseConnector) ---

/** Represents the final, structured record ready to be written to the Bronze
  * Layer.
  */
case class BronzeRecord(
    articleId: String,
    chunkId: String,
    content: String
)

// NOTE: No implicit Circe codec needed here unless the record is serialized for network transit.
