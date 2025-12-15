package com.webcat.hotovo.infrastructure

import zio._
import zio.config._
import zio.Config._
import zio.ConfigProvider
import sttp.client4._
import sttp.client4.asStringAlways // Needed for raw string response
import sttp.client4.circe._
import sttp.client4.httpclient.zio.HttpClientZioBackend
import io.circe._
import io.circe.parser._
import io.circe.generic.semiauto._
import io.circe.syntax._

/** Defines the request payload structure for the Ollama /api/generate endpoint.
  */
case class OllamaRequest(
    model: String,
    prompt: String,
    stream: Boolean = false
)

// Circe Encoder for serialization to JSON
object OllamaRequest {
  implicit val encoder: Encoder[OllamaRequest] = deriveEncoder
}

/** The structured response from Ollama that dictates the next action and
  * provides keys for storing the chunk in the Bronze layer.
  *
  * controlAction can be: "CONTINUE", "NEW_ARTICLE", "IRRELEVANT"
  */
case class OllamaBronzeTaggingResponse(
    chunkId: String, // Unique ID for this specific chunk
    articleId: String, // ID used to group chunks into an article (new or existing)
    controlAction: String // "CONTINUE", "NEW_ARTICLE", "IRRELEVANT"
)

object OllamaBronzeTaggingResponse {
  // Circe Decoder
  implicit val decoder: Decoder[OllamaBronzeTaggingResponse] = deriveDecoder
}

/** Intermediate structure to parse the *outer* JSON envelope from the Ollama
  * /api/generate endpoint, containing the raw LLM output.
  */
case class OllamaIntermediateResponse(response: String)
// Circe Decoder for deserialization from the outer JSON
object OllamaIntermediateResponse {
  implicit val decoder: Decoder[OllamaIntermediateResponse] = deriveDecoder
}

// --- ZIO Service Definition (Trait) ---

/** Defines the OllamaClient service interface.
  */
trait OllamaClient {

  /** Generates a text interpretation from the model.
    */
  def getInterpretation(prompt: String): Task[String]

  /** Queries Ollama for relevance and grouping identifiers for the Bronze
    * layer.
    * @param currentArticleId
    *   The ID of the article currently being processed (or "NEW").
    * @param htmlChunk
    *   The HTML content chunk to analyze.
    * @return
    *   A structured response containing the chunk's unique ID and its article
    *   ID.
    */
  def tagBronzeChunk(
      currentArticleId: String,
      htmlChunk: String
  ): Task[OllamaBronzeTaggingResponse]
}

// --- Implementation (Concrete Class) ---

/** Concrete implementation of the OllamaClient service.
  */
class OllamaClientLive(
    serverUrl: String,
    modelName: String,
    backend: HttpClientZioBackend
) extends OllamaClient {

  // --- Core Ollama API Interaction ---

  /** Sends a prompt to the Ollama model via HTTP POST, returning a ZIO effect.
    */
  private def generate(prompt: String): Task[String] = {
    val requestPayload = OllamaRequest(modelName, prompt)
    val endpoint = s"$serverUrl/api/generate"

    val request = basicRequest
      .post(uri"$endpoint")
      .header("Content-Type", "application/json")
      .body(
        requestPayload.asJson.noSpaces
      ) // Use Circe Encoder
      .response(
        asJson[OllamaIntermediateResponse]
      )

    // Use the injected backend to send the request
    request.send(backend).flatMap { response =>
      response.body match {
        case Right(intermediateRes) => ZIO.succeed(intermediateRes.response)

        case Left(error) =>
          ZIO.fail(
            new RuntimeException(
              s"Ollama request failed: ${error.toString}. Error detail: ${response.code}"
            )
          )
      }
    }
  }

  // --- Service Methods Implementation ---

  override def getInterpretation(
      prompt: String
  ): ZIO[Any, Throwable, String] = {
    generate(prompt)
  }

  // --- Web Interpretation Logic (Prompt Engineering) ---

  private val bronzeTaggingPrompt: String =
    """You are a stateless, expert data extraction tagger for a geopolitical news pipeline.
    Your task is to analyze a single chunk of HTML content and provide grouping identifiers
    for storage in the Bronze layer.

    Input:
    1. CURRENT_ARTICLE_ID: The unique ID of the article being assembled. Use 'NEW' if no article is currently open.
    2. HTML_CHUNK: The new chunk of text.

    Rules:
    1. Generate a NEW, globally unique identifier for 'chunkId' (e.g., a UUID).
    2. If the chunk is NOT relevant to geopolitics, set 'control_action' to "IRRELEVANT" and keep 'articleId' as CURRENT_ARTICLE_ID.
    3. If the chunk IS relevant and CURRENT_ARTICLE_ID is 'NEW', set 'control_action' to "NEW_ARTICLE" and generate a NEW unique ID for 'articleId'.
    4. If the chunk IS relevant and CURRENT_ARTICLE_ID is a valid ID, set 'control_action' to "CONTINUE" and use the CURRENT_ARTICLE_ID.

    Output MUST be a single JSON object matching the schema:
    {"chunkId": "...", "articleId": "...", "controlAction": "..."}
    """.stripMargin

  override def tagBronzeChunk(
      currentArticleId: String,
      htmlChunk: String
  ): ZIO[Any, Throwable, OllamaBronzeTaggingResponse] = {

    val userPrompt =
      s"CURRENT_ARTICLE_ID: $currentArticleId\n\nHTML_CHUNK:\n$htmlChunk"

    val fullPrompt = s"$bronzeTaggingPrompt\n\n$userPrompt"

    generate(fullPrompt) // Returns Task[String] (the raw JSON string)
      .flatMap { jsonString =>
        // Decode the simple Bronze Tagging structure
        decode[OllamaBronzeTaggingResponse](jsonString) match {
          case Right(control) => ZIO.succeed(control)
          case Left(error)    =>
            ZIO.fail(
              new RuntimeException(
                s"Failed to parse Ollama Bronze Tagging JSON response. Response: '$jsonString'. Error: ${error.getMessage}"
              )
            )
        }
      }
  }
}

// --- Configuration and ZLayer Definition ---

/** Configuration for the Ollama server details.
  */
case class OllamaConfig(serverUrl: String, modelName: String = "llama3")

// Companion object of the OllamaClient trait (acts as a type).
object OllamaClient {

  // --- ZIO Config Definition ---

  // Blueprint to create a config ZLayer.
  private val ollamaConfigSpec: Config[OllamaConfig] =
    (
      string("OLLAMA_SERVER_URL") ?? "URL of the Ollama server" zip
        string("OLLAMA_MODEL_NAME") ?? "The specific Ollama model to use"
    ).to[OllamaConfig]

  // Turns the blueprint into a ZLayer.
  val configProvider: ConfigProvider = ConfigProvider.defaultProvider
  val configLayer: ZLayer[Any, Config.Error, OllamaConfig] = ZLayer.fromZIO(
    configProvider
      .load(ollamaConfigSpec)
      .tapError(e =>
        ZIO.logError(
          s"Configuration error loading OllamaConfig: ${e.getMessage}"
        )
      )
  )

  // --- Live ZLayer ---
  val live: ZLayer[
    OllamaConfig with HttpClientZioBackend,
    Throwable,
    OllamaClient
  ] =
    ZLayer {
      for {
        config <- ZIO.service[OllamaConfig]
        backend <- ZIO.service[HttpClientZioBackend]
      } yield new OllamaClientLive(config.serverUrl, config.modelName, backend)
    }

  // Accessor method for the primary Bronze Tagging function
  def tagBronzeChunk(
      currentArticleId: String,
      htmlChunk: String
  ): ZIO[OllamaClient, Throwable, OllamaBronzeTaggingResponse] =
    ZIO.serviceWithZIO[OllamaClient](
      _.tagBronzeChunk(currentArticleId, htmlChunk)
    )
}
