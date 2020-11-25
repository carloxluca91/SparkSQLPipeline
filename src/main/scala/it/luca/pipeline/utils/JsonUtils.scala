package it.luca.pipeline.utils

import argonaut._
import Argonaut._
import it.luca.pipeline.exception.{JsonFileParsingException, JsonStringParsingException}
import org.apache.log4j.Logger

import scala.io.{BufferedSource, Source}
import scala.reflect.runtime.universe._

object JsonUtils {

  private final val logger = Logger.getLogger(getClass)

  final def decodeJsonFile[T](jsonFilePath: String)(implicit decodeJson: DecodeJson[T], typeTag: TypeTag[T]): T = {

    val tClassName: String = typeOf[T].typeSymbol.name.toString
    logger.info(s"Trying to parse provided json file ($jsonFilePath) as an object of type $tClassName")
    val bufferedSource: BufferedSource = Source.fromFile(jsonFilePath, "UTF-8")
    val pipelineJsonString = bufferedSource.getLines().mkString
    bufferedSource.close()

    pipelineJsonString.decodeOption[T] match {
      case None =>
        logger.error(s"Unable to parse provided json file ($jsonFilePath) ")
        throw JsonFileParsingException(jsonFilePath, tClassName)
      case Some(value) =>
        logger.info(s"Successfully parsed provided json file ($jsonFilePath) as an object of type $tClassName")
        value
    }
  }

  final def decodeJsonString[T](jsonString: String)(implicit decodeJson: DecodeJson[T], typeTag: TypeTag[T]): T = {

    val tClassName: String = typeOf[T].typeSymbol.name.toString
    logger.info(s"Trying to parse provided json string as an object of type $tClassName")
    jsonString.decodeOption[T] match {
      case None =>
        logger.error(s"Unable to parse provided json string as an object of type $tClassName")
        throw JsonStringParsingException(tClassName)
      case Some(value) =>
        logger.info(s"Successfully parsed provided json string as an object of type $tClassName")
        value
    }
  }

  final def decodeAndInterpolateJsonFile[T](jsonFilePath: String, jobProperties: JobProperties)
                                           (implicit decodeJson: DecodeJson[T], typeTag: TypeTag[T]): T = {

    val propertyValueRegex = "\"\\$\\{([\\w|.]+)}\"".r
    val tClassName: String = typeOf[T].typeSymbol.name.toString
    logger.info(s"Trying to parse provided json file ($jsonFilePath) as an object of type $tClassName")

    // Open provided json file path, make it a single-line string and perform property interpolation
    val bufferedSource: BufferedSource = Source.fromFile(jsonFilePath, "UTF-8")
    val pipelineJsonString = bufferedSource.getLines().mkString
    val pipelineJonStringWithInterpolatedProperties: String = propertyValueRegex
      .replaceAllIn(pipelineJsonString, m => s""""${jobProperties.get(m.group(1))}"""")
    bufferedSource.close()

    // Try to parse the interpolated json string as an object of type T
    pipelineJonStringWithInterpolatedProperties.decodeOption[T] match {
      case None =>
        logger.error(s"Unable to parse provided json file ($jsonFilePath) ")
        throw JsonFileParsingException(jsonFilePath, tClassName)
      case Some(value) =>
        logger.info(s"Successfully parsed provided json file ($jsonFilePath) as an object of type $tClassName")
        value
    }
  }
}
