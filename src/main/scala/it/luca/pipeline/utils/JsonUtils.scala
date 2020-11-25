package it.luca.pipeline.utils

import argonaut._, Argonaut._
import it.luca.pipeline.exception.{JsonFileParsingException, JsonStringParsingException, UnexistingKeyException}
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger

import scala.io.{BufferedSource, Source}
import scala.reflect.runtime.universe._
import scala.util.matching.Regex

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

  @throws[UnexistingKeyException]
  @throws[JsonFileParsingException]
  final def decodeAndInterpolateJsonFile[T](jsonFilePath: String, jobProperties: PropertiesConfiguration)
                                           (implicit decodeJson: DecodeJson[T], typeTag: TypeTag[T]): T = {

    // Open provided json file path, make it a single-line string and perform property interpolation
    val bufferedSource: BufferedSource = Source.fromFile(jsonFilePath, "UTF-8")
    val pipelineJsonString = bufferedSource.getLines().mkString
    val getPropertyValue: String => String =
      key => Option(jobProperties.getString(key)) match {
        case None => throw UnexistingKeyException(key)
        case Some(x) => x
      }

    // Try to parse the interpolated json string as an object of type T
    val tClassName: String = typeOf[T].typeSymbol.name.toString
    val propertyValueRegex: Regex = "\"\\$\\{([\\w|.]+)}\"".r
    val interpolatedPipelineJonString: String = propertyValueRegex
      .replaceAllIn(pipelineJsonString, m => s""""${getPropertyValue(m.group(1))}"""")
    bufferedSource.close()
    logger.info(s"Successfully interpolated whole json string from file $jsonFilePath. Now, trying to parse it as an object of type $tClassName")

    interpolatedPipelineJonString.decodeOption[T] match {
      case None =>
        logger.error(s"Unable to parse json string from file $jsonFilePath")
        throw JsonFileParsingException(jsonFilePath, tClassName)
      case Some(value) =>
        logger.info(s"Successfully parsed json string from file $jsonFilePath as an object of type $tClassName")
        value
    }
  }
}