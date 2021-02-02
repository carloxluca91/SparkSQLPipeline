package it.luca.pipeline

import it.luca.pipeline.option.ScoptParser
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object Main extends App {

  val log = Logger.getLogger(getClass)
  log.info("Starting main application")
  ScoptParser.configParser.parse(args, ScoptParser.InputConfiguration()) match {
    case None => log.error("Error on parsing main arguments")
    case Some(value) =>

      log.info(s"Successfully parsed main arguments $value")
      Try {

        PipelineRunner(value).run()

      } match {
        case Failure(exception) => log.error(s"Caught error while trying to kick off pipeline '${value.pipelineName}'. Stack trace: ", exception)
        case Success(_) => log.info("Exiting main application")
      }
  }
}
