package it.luca.pipeline.test

import argonaut.Argonaut._
import argonaut._
import org.apache.log4j.Logger

import scala.reflect.runtime.universe._

abstract class JsonSpec extends AbstractSpec {

  private final val logger = Logger.getLogger(getClass)

  def toJsonString[T](tObject: T)(implicit encodeJson: EncodeJson[T], typeTag: TypeTag[T]): String = {

    val jsonString: String = tObject.jencode.spaces4
    logger.info(s"Json representation of input object of type ${className[T]}: \n\n$jsonString\n")
    jsonString
  }
}
