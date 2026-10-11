package newspipe.control.entryPoint

import com.typesafe.scalalogging.LazyLogging
import newspipe.control.dependencyInjection.AppContainer
import newspipe.infrastructure.env.Env

import java.time.{Duration, Instant}

object Main extends LazyLogging:
  def main(args: Array[String]): Unit =
    val env = Env.load()
    val app = new AppContainer(env)
    sys.addShutdownHook { app.close() }
    val poll = Duration.ofSeconds(env.pollIntervalSeconds)
    logger.info(s"newspipe-enrichment polling every $poll")
    while true do
      try
        val n = app.service.runOnce(Instant.now())
        logger.info(s"poll merged $n enriched units")
      catch case e: Exception => logger.error("poll iteration failed", e)
      Thread.sleep(poll.toMillis)
