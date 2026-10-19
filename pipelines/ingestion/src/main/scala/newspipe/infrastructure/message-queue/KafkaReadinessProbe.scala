package newspipe.infrastructure.messagequeue

import newspipe.application.ports.{KafkaReadinessProbe, ProbeResult}

import java.net.{InetSocketAddress, Socket}
import scala.util.Try

final class TcpKafkaReadinessProbe(bootstrap: String, timeoutMillis: Int) extends KafkaReadinessProbe:
  def name: String = "kafka"

  def check(): ProbeResult =
    val endpoints: List[String] = bootstrap.split(",").toList.map(_.trim).filter(_.nonEmpty)
    if endpoints.isEmpty then ProbeResult.NotReady(name, "no bootstrap server configured")
    else
      val results: List[Either[String, String]] = endpoints.map(reachable)
      results.collectFirst { case Left(detail) => detail } match
        case Some(detail) => ProbeResult.NotReady(name, detail)
        case None => ProbeResult.Ready

  private def reachable(endpoint: String): Either[String, String] =
    val idx: Int = endpoint.lastIndexOf(':')
    if idx <= 0 then Left(s"malformed bootstrap '$endpoint'")
    else
      val host: String = endpoint.substring(0, idx)
      val port: Int = Try(endpoint.substring(idx + 1).toInt).getOrElse(-1)
      if port <= 0 then Left(s"malformed port in '$endpoint'")
      else
        Try:
          val socket: Socket = new Socket()
          try socket.connect(new InetSocketAddress(host, port), timeoutMillis)
          finally socket.close()
          endpoint
        .toEither.left.map(_ => s"cannot reach $endpoint")
