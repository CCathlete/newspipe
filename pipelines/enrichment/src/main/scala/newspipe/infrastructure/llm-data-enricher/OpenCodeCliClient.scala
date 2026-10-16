package newspipe.infrastructure.llmdataenricher

import io.circe.parser.*
import newspipe.application.ports.EnrichmentPort
import newspipe.domain.models.{Enrichment, RssItem}
import newspipe.domain.valueObjects.{GeoArea, Urgency}

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

final class OpenCodeCliClient(model: String, timeout: FiniteDuration) extends EnrichmentPort:

  def enrich(item: RssItem): Either[Throwable, Enrichment] =
    Try {
      val prompt = OpenCodeCliClient.promptFor(item)
      val proc = new ProcessBuilder("opencode", "run", "--format", "json", "--model", model, prompt)
        .redirectErrorStream(false)
        .start()
      val startNanos = System.nanoTime()
      val finished = proc.waitFor(timeout.toMillis, TimeUnit.MILLISECONDS)
      val elapsedSecs = (System.nanoTime() - startNanos) / 1000000000L
      if !finished then
        OpenCodeCliClient.killTree(proc)
        throw new RuntimeException(s"opencode run timed out after $elapsedSecs s; skipped item '${item.link}'")
      val stdout = new String(proc.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      if proc.exitValue() != 0 then
        throw new RuntimeException(s"opencode run exited ${proc.exitValue()}")
      OpenCodeCliClient.parseEvents(stdout)
    }.toEither

object OpenCodeCliClient:
  def killTree(proc: Process): Unit =
    proc.descendants().forEach(_.destroyForcibly())
    proc.destroyForcibly()
    try proc.getInputStream.close() catch case _: Exception => ()
    try proc.getErrorStream.close() catch case _: Exception => ()
    try proc.getOutputStream.close() catch case _: Exception => ()
    val _ = proc.waitFor(5, TimeUnit.SECONDS)

  def promptFor(item: RssItem): String =
    val stream = getClass.getClassLoader.getResourceAsStream("prompts/enrich.txt")
    require(stream != null, "prompts/enrich.txt missing from classpath")
    val template =
      try scala.io.Source.fromInputStream(stream, "UTF-8").mkString
      finally stream.close()
    template
      .replace("{{TITLE}}", item.title)
      .replace("{{LINK}}", item.link)
      .replace("{{DESCRIPTION}}", item.description)

  def parseEvents(ndjson: String): Enrichment =
    val text = ndjson.linesIterator.flatMap { line =>
      parse(line).toOption.flatMap(_.hcursor.downField("part").downField("text").as[String].toOption)
    }.mkString
    val start = text.indexOf('{')
    val end = text.lastIndexOf('}')
    if start < 0 || end <= start then
      throw new RuntimeException(s"no JSON object in opencode output: ${text.take(200)}")
    parseEnrichment(text.substring(start, end + 1))

  def parseEnrichment(json: String): Enrichment =
    val doc = parse(json).getOrElse(throw new RuntimeException(s"invalid enrichment JSON: ${json.take(200)}"))
    val c = doc.hcursor
    Enrichment(
      summary = c.downField("summary").as[String].getOrElse(""),
      countries = c.downField("countries").as[List[String]].getOrElse(Nil),
      geoArea = GeoArea(c.downField("geoArea").as[String].getOrElse("unknown")),
      topics = c.downField("topics").as[List[String]].getOrElse(Nil),
      actors = c.downField("actors").as[List[String]].getOrElse(Nil),
      urgency = Urgency(c.downField("urgency").as[String].getOrElse("low")),
      language = c.downField("language").as[String].getOrElse("en")
    )
