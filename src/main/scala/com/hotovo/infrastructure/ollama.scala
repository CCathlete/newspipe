package com.hotovo.infrastructure

import zio._
import sttp.client4._
import sttp.client4.circe._
import sttp.client4.httpclient.zio.HttpClientZioBackend
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._

/** Defines the request payload structure for the Ollama /api/generate endpoint.
  * Uses Circe derivation for Encoders.
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

/** Service to interact with the Ollama server for AI interpretation, using ZIO
  * and Circe.
  */
class OllamaClient(
    val serverUrl: String,
    val modelName: String = "llama3"
) {

  private type OllamaEnv = HttpClientZioBackend

  // --- Core Ollama API Interaction ---

  /** Sends a prompt to the Ollama model via HTTP POST, returning a ZIO effect.
    */
  private def generate(prompt: String): ZIO[OllamaEnv, Throwable, String] = {
    val requestPayload = OllamaRequest(modelName, prompt)
    val endpoint = s"$serverUrl/api/generate"

    val request = basicRequest
      .post(uri"$endpoint")
      .header("Content-Type", "application/json")
      .body(
        requestPayload.asJson.noSpaces
      ) // Use Circe Encoder to serialize body
      .response(asJson[OllamaResponse]) // Use Circe Decoder for response

    request.send(HttpClientZioBackend()).flatMap { response =>
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

  /** Generates a text interpretation from the model.
    */
  def getInterpretation(prompt: String): ZIO[OllamaEnv, Throwable, String] = {
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

  def isChunkRelevant(htmlChunk: String): ZIO[OllamaEnv, Throwable, Boolean] = {
    val fullPrompt = s"$relevanceSystemPrompt\n\nHTML Content:\n$htmlChunk"

    getInterpretation(fullPrompt).map { response =>
      response.trim.toUpperCase == "RELEVANT"
    }
  }

  def extractHeadlineAndSummary(
      relevantHtmlChunk: String
  ): ZIO[OllamaEnv, Throwable, String] = {
    val fullPrompt =
      s"$extractionSystemPrompt\n\nHTML Content:\n$relevantHtmlChunk"

    getInterpretation(fullPrompt)
  }
}
