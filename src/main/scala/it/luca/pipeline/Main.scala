package it.luca.pipeline

import it.luca.pipeline.option.ScoptParser
import org.apache.log4j.Logger

object Main extends App {

  val logger = Logger.getLogger(getClass)

  logger.info("Starting application main")

  ScoptParser.configParser.parse(args, ScoptParser.Config()) match {
    case None => logger.error("Error on parsing main arguments")
    case Some(config) =>

      logger.info(s"Successfully parsed main arguments. ${config.toString}")
  }
}
