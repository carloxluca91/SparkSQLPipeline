package it.luca.pipeline

import it.luca.pipeline.option.ScoptParser
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object Main extends App {

  val logger = Logger.getLogger(getClass)

  logger.info("Starting application main")

  ScoptParser.configParser.parse(args, ScoptParser.Config()) match {
    case None => logger.error("Error on parsing main arguments")
    case Some(config) =>

      logger.info(s"Successfully parsed main arguments. $config")
      Try {

        PipelineRunner.run(config.pipelineName, config.jobPropertiesFile)

      } match {
        case Failure(exception) =>
          logger.error(s"Caught error while trying to run pipeline ${config.pipelineName}. " +
          s"Stack trace: ", exception)
        case Success(_) =>
      }
  }
}
