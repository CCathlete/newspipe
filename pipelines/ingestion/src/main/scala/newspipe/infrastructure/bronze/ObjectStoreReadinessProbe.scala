package newspipe.infrastructure.bronze

import newspipe.application.ports.{ObjectStoreReadinessProbe, ProbeResult}

import java.net.{HttpURLConnection, URI}
import scala.util.Try

final class HttpObjectStoreReadinessProbe(
    endpoint: String,
    bucket: String,
    timeoutMillis: Int
) extends ObjectStoreReadinessProbe:
  def name: String = "object-store"

  def check(): ProbeResult =
    if endpoint.isEmpty then ProbeResult.NotReady(name, "no endpoint configured")
    else if bucket.isEmpty then ProbeResult.NotReady(name, "no bucket configured")
    else
      val health: URI = URI.create(s"$endpoint/health")
      Try:
        val conn: HttpURLConnection = health.toURL.openConnection().asInstanceOf[HttpURLConnection]
        try
          conn.setRequestMethod("GET")
          conn.setConnectTimeout(timeoutMillis)
          conn.setReadTimeout(timeoutMillis)
          conn.connect()
          val code: Int = conn.getResponseCode
          conn.disconnect()
          code
        catch case e: Exception => throw e
      match
        case scala.util.Success(code) if code >= 200 && code < 400 => ProbeResult.Ready
        case scala.util.Success(code) => ProbeResult.NotReady(name, s"liveness probe at $endpoint/health returned HTTP $code")
        case scala.util.Failure(e) => ProbeResult.NotReady(name, s"cannot reach $endpoint: ${e.getMessage}")
