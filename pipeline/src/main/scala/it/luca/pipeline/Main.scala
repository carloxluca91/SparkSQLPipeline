package it.luca.pipeline

import it.luca.pipeline.option.ScoptParser
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object Main extends App {

  val logger = Logger.getLogger(getClass)

  logger.info("Starting main application")

  ScoptParser.configParser.parse(args, ScoptParser.InputConfiguration()) match {
    case None => logger.error("Error on parsing main arguments")
    case Some(value) =>

      logger.info(s"Successfully parsed main arguments $value")
      Try {

        PipelineRunner(value).run()

      } match {
        case Failure(exception) =>
          logger.error(s"Caught error while trying to kick off pipeline '${value.pipelineName}'. Stack trace: ", exception)
        case Success(_) =>
          logger.info("Exiting main application")
      }
  }
}
