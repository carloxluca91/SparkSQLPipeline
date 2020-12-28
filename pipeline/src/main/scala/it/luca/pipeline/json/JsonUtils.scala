package it.luca.pipeline.json

import argonaut._, Argonaut._
import it.luca.pipeline.exception.{JsonDecodingException, JsonSyntaxException, UnexistingPropertyException}
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger

import scala.io.{BufferedSource, Source}
import scala.reflect.runtime.universe._

object JsonUtils {

  private final val logger = Logger.getLogger(getClass)

  private def checkJsonSyntax(jsonString: String): Unit = {

    logger.info("Checking .json syntax correctness for provided string")
    jsonString.parseOption match {
      case None => throw JsonSyntaxException(jsonString)
      case Some(x) => logger.info(s"Provided .json string is correct. (Pretty) result: \n\n${x.spaces4}\n")
    }
  }

  final def decodeJsonFile[T](jsonFilePath: String)(implicit decodeJson: DecodeJson[T], typeTag: TypeTag[T]): T = {

    val bufferedSource: BufferedSource = Source.fromFile(jsonFilePath, "UTF-8")
    val jsonString = bufferedSource.getLines().mkString
    bufferedSource.close()
    decodeJsonString[T](jsonString)
  }

  final def decodeJsonString[T](jsonString: String)(implicit decodeJson: DecodeJson[T], typeTag: TypeTag[T]): T = {

    checkJsonSyntax(jsonString)
    val tClassName: String = typeOf[T].typeSymbol.name.toString
    logger.info(s"Now, trying to parse it as an object of type $tClassName")
    jsonString.decodeOption[T] match {
      case None => throw JsonDecodingException[T](jsonString)
      case Some(value) =>
        logger.info(s"Successfully parsed provided json string as an object of type $tClassName")
        value
    }
  }

  final def decodeAndInterpolateJsonFile[T](jsonFilePath: String, jobProperties: PropertiesConfiguration)
                                           (implicit decodeJson: DecodeJson[T], typeTag: TypeTag[T]): T = {

    // Open provided json file path, make it a single-line string and perform property interpolation
    val bufferedSource: BufferedSource = Source.fromFile(jsonFilePath, "UTF-8")
    val jsonString = bufferedSource.getLines().mkString
    bufferedSource.close()
    decodeAndInterpolateJsonString[T](jsonString, jobProperties)
  }

  final def decodeAndInterpolateJsonString[T](jsonString: String, jobProperties: PropertiesConfiguration)
                                           (implicit decodeJson: DecodeJson[T], typeTag: TypeTag[T]): T = {

    val getPropertyValue: String => String =
      key => Option(jobProperties.getString(key)) match {
        case None => throw UnexistingPropertyException(key)
        case Some(x) => x
      }

    val interpolatedJsonString: String = "\"\\$\\{([\\w|.]+)}\"".r
      .replaceAllIn(jsonString, m => s""""${getPropertyValue(m.group(1))}"""")

    checkJsonSyntax(interpolatedJsonString)
    decodeJsonString[T](interpolatedJsonString)
  }
}
