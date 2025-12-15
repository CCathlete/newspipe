package com.webcat.hotovo.infrastructure

import zio._
import zio.config._
import sttp.client4._
import sttp.client4.circe._
import sttp.client4.httpclient.zio.HttpClientZioBackend
import io.circe._
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

/** Defines the successful response structure from the Ollama /api/generate
  * endpoint. Uses Circe derivation for Decoders.
  */
case class OllamaResponse(response: String)
// Circe Decoder for deserialization from JSON
object OllamaResponse {
  implicit val decoder: Decoder[OllamaResponse] = deriveDecoder
}

// --- ZIO Service Definition (Trait) ---

/** Defines the OllamaClient service interface.
  */
trait OllamaClient {

  /** Generates a text interpretation from the model.
    */
  def getInterpretation(prompt: String): ZIO[Any, Throwable, String]

  /** Checks if an HTML chunk is relevant to geopolitics.
    */
  def isChunkRelevant(htmlChunk: String): ZIO[Any, Throwable, Boolean]

  /** Extracts the headline and summary from a relevant HTML chunk.
    */
  def extractHeadlineAndSummary(
      relevantHtmlChunk: String
  ): ZIO[Any, Throwable, String]
}

// --- Implementation (Concrete Class) ---

/** Concrete implementation of the OllamaClient service.
  *
  * It depends on a Configuration for the server details and the
  * HttpClientZioBackend. The ZIO environment for its methods is 'Any' because
  * the dependencies (backend) are managed by the ZLayer.
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
      .response(asJson[OllamaResponse]) // Use Circe Decoder

    // Use the injected backend to send the request
    request.send(backend).flatMap { response =>
      response.body match {
        case Right(ollamaRes) => ZIO.succeed(ollamaRes.response)
        case Left(error)      =>
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

  private val relevanceSystemPrompt: String =
    """You are an expert news article analyzer focused on geopolitics.
    Your task is to determine if a given chunk of HTML text is highly relevant
    to geopolitics, international relations, or specific country conflicts.
    Respond ONLY with 'RELEVANT' if it is, or 'IRRELEVANT' if it is not.
    Do not add any other explanation, punctuation, or text.
    """.stripMargin

  private val extractionSystemPrompt: String =
    """You are an expert data extraction agent.
    From the provided HTML text, you must extract the article headline and a concise,
    three-sentence summary.
    Format your output strictly as a JSON object with two keys:
    {"headline": "...", "summary": "..."}
    """.stripMargin

  override def isChunkRelevant(
      htmlChunk: String
  ): Task[Boolean] = {
    val fullPrompt = s"$relevanceSystemPrompt\n\nHTML Content:\n$htmlChunk"

    getInterpretation(fullPrompt).map { response =>
      response.trim.toUpperCase == "RELEVANT"
    }
  }

  override def extractHeadlineAndSummary(
      relevantHtmlChunk: String
  ): ZIO[Any, Throwable, String] = {
    val fullPrompt =
      s"$extractionSystemPrompt\n\nHTML Content:\n$relevantHtmlChunk"

    getInterpretation(fullPrompt)
  }
}

// --- Configuration and ZLayer Definition ---

/** Configuration for the Ollama server details.
  */
case class OllamaConfig(serverUrl: String, modelName: String = "llama3")

object OllamaClient {
  type OllamaClientEnv = OllamaClient

  // --- ZIO Config Definition ---

  // 1. Define a ZIO Config Descriptor: Tells ZIO Config how to find and parse the values.
  private val ollamaConfigDescriptor: Config =
    (ConfigDescriptor.string("serverUrl")
      ?? "URL of the Ollama server, e.g., http://localhost:11434")
      .zip(
        ConfigDescriptor
          .string("modelName")
          .default("llama3") ?? "The specific Ollama model to use"
      )
      .toFunction(
        OllamaConfig.apply _
      ) // Automatically map the configuration to the case class

  // 2. The Configuration ZLayer: Loads the config from the default sources (e.g., system props, env vars).
  // E=Config.Error (A type of Throwable), R=Any
  val configLayer: ZLayer[Any, Config.Error, OllamaConfig] =
    ZLayer.fromZIO(
      ZIO
        .config(ollamaConfigDescriptor)
        .tapError(e =>
          ZIO.logError(
            s"Configuration error loading OllamaConfig: ${e.message}"
          )
        )
    )

  // 3. The Live ZLayer (Requires OllamaConfig from above)
  val live
      : ZLayer[OllamaConfig & HttpClientZioBackend, Nothing, OllamaClientEnv] =
    ZLayer {
      for {
        config <- ZIO.service[OllamaConfig]
        backend <- ZIO.service[HttpClientZioBackend]
      } yield new OllamaClientLive(config.serverUrl, config.modelName, backend)
    }

  // --- Accessor Methods (Updated to use the type-safe ArticleData) ---

  def getInterpretation(
      prompt: String
  ): ZIO[OllamaClientEnv, Throwable, String] =
    ZIO.serviceWithZIO[OllamaClientEnv](_.getInterpretation(prompt))

  def isChunkRelevant(
      htmlChunk: String
  ): ZIO[OllamaClientEnv, Throwable, Boolean] =
    ZIO.serviceWithZIO[OllamaClientEnv](_.isChunkRelevant(htmlChunk))

  def extractHeadlineAndSummary(
      relevantHtmlChunk: String
  ): ZIO[OllamaClientEnv, Throwable, ArticleData] = // Updated return type
    ZIO.serviceWithZIO[OllamaClientEnv](
      _.extractHeadlineAndSummary(relevantHtmlChunk)
    )
}
