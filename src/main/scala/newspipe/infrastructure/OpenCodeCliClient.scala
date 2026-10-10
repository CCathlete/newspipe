package newspipe.infrastructure

import io.circe.parser.*
import newspipe.domain.{Enrichment, EnrichmentPort, RssItem}

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
      val secs = timeout.toSeconds
      val finished = proc.waitFor(secs, TimeUnit.SECONDS)
      val stdout = new String(proc.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      if !finished then
        proc.destroyForcibly()
        throw new RuntimeException(s"opencode run timed out after $secs s")
      if proc.exitValue() != 0 then
        throw new RuntimeException(s"opencode run exited ${proc.exitValue()}")
      OpenCodeCliClient.parseEvents(stdout)
    }.toEither

object OpenCodeCliClient:
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
      geoArea = c.downField("geoArea").as[String].getOrElse("unknown"),
      topics = c.downField("topics").as[List[String]].getOrElse(Nil),
      actors = c.downField("actors").as[List[String]].getOrElse(Nil),
      urgency = c.downField("urgency").as[String].getOrElse("low"),
      language = c.downField("language").as[String].getOrElse("en")
    )
