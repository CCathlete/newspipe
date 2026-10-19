package newspipe.control.entryPoint

import com.typesafe.scalalogging.LazyLogging
import newspipe.control.dependencyInjection.AppContainer
import newspipe.infrastructure.env.{Env, EnvConfig}

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

    logger.info("newspipe-ingestion polling " + env.feeds.size + " feeds every " + poll)
    while true do
      try
        val n = app.service.runOnce(Instant.now())
        logger.info("poll wrote " + n + " bronze units")
      catch case e: Exception => logger.error("poll iteration failed", e)
      Thread.sleep(poll.toMillis)
