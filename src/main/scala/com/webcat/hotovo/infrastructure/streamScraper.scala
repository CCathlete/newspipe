package com.webcat.hotovo.infrastructure

import zio._
import zio.stream.ZStream
import sttp.client4._
import sttp.capabilities.zio.ZioStreams
import sttp.client4.httpclient.zio.HttpClientZioBackend
import sttp.model.Uri

// --- ZIO Service Definition (Trait) ---

/** Defines the service responsible for fetching web content and preparing it
  * for the Ollama processing stage via a stream of chunks.
  */
trait ScrapingService {

  /** Fetches a URL and streams the HTML content as small, sequential chunks
    * directly from the network response body.
    *
    * @param url
    *   The URL of the news article to scrape.
    * @return
    *   A ZStream of String, where each String is a raw piece of HTML content.
    */
  def scrapeAndChunk(url: String): ZStream[Any, Throwable, String]
}

// --- Implementation (Concrete Class) ---

class ScrapingServiceLive(backend: HttpClientZioBackend)
    extends ScrapingService {

  private val OllamaChunkSize: Int = 4096

  override def scrapeAndChunk(
      url: String
  ): ZStream[Any, Throwable, String] = {

    val uri: Uri = uri"$url"

    // 1. Define the ZIO[Scope, ...] effect that performs the request and returns the ZStream[Byte]
    val responseStreamZIO
        : ZIO[Scope, Throwable, ZStream[Any, Throwable, Byte]] =
      for {
        // Send the request and get the response
        response <- basicRequest
          .get(uri)
          .response(
            asStreamUnsafe(ZioStreams)
          ) // Returns a scoped resource
          .send(backend)

        // Check the body for success or failure
        stream <- response.body match {
          case Right(s) =>
            ZIO.succeed(s) // Success: The acquired ZStream[Byte]
          case Left(error) =>
            ZIO.fail(new RuntimeException(s"HTTP Error: $error"))
        }
      } yield stream // This returns ZIO[Scope, Throwable, ZStream[Byte]]

    // 2. Use ZStream.scoped to acquire the stream and manage the underlying resource (HTTP connection)
    ZStream
      .scoped(responseStreamZIO)
      .flatten
      .chunks
      .map(_.toArray)
      .map(new String(_, "UTF-8"))
      .flatMap(fullString => {
        val chunks = fullString.grouped(OllamaChunkSize).toSeq
        ZStream.fromIterable(chunks)
      })
      .tapError(e =>
        ZIO.logError(
          s"Scoped streaming scrape failed for $url: ${e.getMessage}"
        )
      )
  }
}

// --- Companion Object and ZLayer Definition ---

object ScrapingService {

  // Accessor method for stream-based consumption
  def scrapeAndChunk(url: String): ZStream[ScrapingService, Throwable, String] =
    ZStream.serviceWithStream[ScrapingService](_.scrapeAndChunk(url))

  // Live Layer construction
  val live: ZLayer[HttpClientZioBackend, Throwable, ScrapingService] =
    ZLayer {
      for {
        backend <- ZIO.service[HttpClientZioBackend]
      } yield new ScrapingServiceLive(backend)
    }
}
