package it.luca.pipeline.json

import argonaut.Argonaut._
import argonaut._
import it.luca.pipeline.exception.{JsonDecodingException, JsonSyntaxException, UnexistingPropertyException}
import it.luca.pipeline.utils.JobProperties
import org.apache.log4j.Logger

import scala.reflect.runtime.universe._

object JsonUtils {

  private val log = Logger.getLogger(getClass)

  private def checkJsonSyntax(jsonString: String): Unit = {

    log.info("Checking .json syntax correctness for provided string")
    jsonString.parseOption match {
      case None => throw JsonSyntaxException(jsonString)
      case Some(x) => log.info(s"Provided .json string is correct. (Pretty) result: \n\n${x.spaces4}\n")
    }
  }

  def decodeJsonString[T](jsonString: String)(implicit decodeJson: DecodeJson[T], typeTag: TypeTag[T]): T = {

    // Check syntax of input string after interpolation and decode
    checkJsonSyntax(jsonString)

    val tClassName: String = typeOf[T].typeSymbol.name.toString
    log.info(s"Now, trying to parse it as an object of type $tClassName")
    jsonString.decodeEither[T] match {
      case Left(a) => throw JsonDecodingException[T](a)
      case Right(value) =>
        log.info(s"Successfully parsed provided json string as an object of type $tClassName")
        value
    }
  }

  def decodeAndInterpolateJsonString[T](jsonString: String, jobProperties: JobProperties)
                                           (implicit decodeJson: DecodeJson[T], typeTag: TypeTag[T]): T = {

    val getPropertyValue: String => String =
      key => Option(jobProperties.getString(key)) match {
        case None => throw UnexistingPropertyException(key)
        case Some(x) => x
      }

    // Check syntax of input string
    checkJsonSyntax(jsonString)

    //noinspection RegExpRedundantEscape
    val interpolatedJsonString: String = "\\$\\{([\\w\\.]+)}".r
      .replaceAllIn(jsonString, m => s"""${getPropertyValue(m.group(1))}""")

    decodeJsonString[T](interpolatedJsonString)
  }
}
