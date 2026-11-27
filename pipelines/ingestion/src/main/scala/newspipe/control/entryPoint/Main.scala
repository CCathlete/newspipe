package newspipe.control.entryPoint

import com.typesafe.scalalogging.LazyLogging
import newspipe.control.dependencyInjection.AppContainer
import newspipe.infrastructure.env.{Env, EnvConfig}
import newspipe.application.ports.RssSource
import newspipe.application.services.IngestionService
import newspipe.application.services.ScanReport

import java.time.{Duration, Instant}

object Main extends LazyLogging:
  def main(args: Array[String]): Unit =
    val env: EnvConfig = Env.load() match
      case Right(e) => e
      case Left(err) => throw new RuntimeException(err)
    val app = new AppContainer(env)
    sys.addShutdownHook { app.close() }
    val poll = Duration.ofSeconds(env.pollIntervalSeconds)
    val report = app.readiness.await(Instant.now())
    if !report.ready then
      logger.error("startup aborted, " + report.describe)
      app.close()
      sys.exit(1)

    val scanReport: ScanReport = app.scan() match
      case Right(report) => report
      case Left(error) => throw new RuntimeException("source scan failed: " + error)

    logger.info("source scan: candidates=" + scanReport.candidates + " usable=" + scanReport.usable +
      " unavailable=" + scanReport.unavailable + " invalid=" + scanReport.invalid +
      " investigation=" + scanReport.investigation)

    val service: IngestionService = app.validatedService(scanReport.usableUrls)

    val rss: RssSource = new newspipe.infrastructure.rss.ValidatedRssSource(scanReport.usableUrls)

    logger.info("newspipe-ingestion polling " + rss.getClass.getSimpleName + " with " + scanReport.usable + " validated feeds every " + poll)

    while true do

      try

        val n = service.runOnce(Instant.now())

        logger.info("poll wrote " + n + " bronze units")

      catch case e: Exception => logger.error("poll iteration failed", e)

      Thread.sleep(poll.toMillis)