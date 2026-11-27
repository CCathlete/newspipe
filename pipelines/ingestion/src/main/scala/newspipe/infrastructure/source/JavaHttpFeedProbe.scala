package newspipe.infrastructure.source

import newspipe.application.ports.FeedProbe

import newspipe.domain.models.ProbeResponse

import java.net.URI

import java.net.http.HttpClient

import java.net.http.HttpRequest

import java.net.http.HttpResponse

import java.time.Duration

import java.util.concurrent.ConcurrentHashMap

import scala.util.Try


final class JavaHttpFeedProbe(timeout: Duration = Duration.ofSeconds(10)) extends FeedProbe:

  private val http: HttpClient = HttpClient.newBuilder()

    .connectTimeout(timeout)

    .followRedirects(HttpClient.Redirect.NORMAL)

    .build()

  private val cache: ConcurrentHashMap[String, Either[String, ProbeResponse]] = ConcurrentHashMap[String, Either[String, ProbeResponse]]()


  def probe(url: String): Either[String, ProbeResponse] =

    cache.computeIfAbsent(url, _ => fetch(url))


  private def fetch(url: String): Either[String, ProbeResponse] =

    Try {

      val request: HttpRequest = HttpRequest.newBuilder(URI.create(url))

        .timeout(timeout)

        .header("User-Agent", "newspipe-scala/0.2")

        .GET()

        .build()

      val response: HttpResponse[String] = http.send(request, HttpResponse.BodyHandlers.ofString())

      ProbeResponse(

        statusCode = response.statusCode(),

        finalUrl = response.uri().toString,

        contentType = response.headers().firstValue("content-type").orElse(""),

        body = response.body()

      )

    }.toEither.left.map(error => "probe failed for " + url + ": " + error.getMessage)