package it.luca.pipeline.test

import argonaut.Argonaut._
import argonaut._
import it.luca.pipeline.utils.JobProperties
import org.apache.log4j.Logger

import scala.reflect.runtime.universe._

trait AbstractJsonSpec extends AbstractSpec {

  private val log = Logger.getLogger(getClass)
  protected final val jobProperties: JobProperties = JobProperties("spark_application.properties")

  final def toJsonString[T](tObject: T)(implicit encodeJson: EncodeJson[T], typeTag: TypeTag[T]): String = {

    val tClassName: String = typeOf[T].typeSymbol.name.toString
    log.info(s"Trying to parse provided object of type $tClassName as a json string")
    val jsonString: String = tObject.jencode.spaces4
    log.info(s"Successfully parsed provided object of type ${className[T]} as a json string. Result: \n\n$jsonString\n")
    jsonString
  }
}
