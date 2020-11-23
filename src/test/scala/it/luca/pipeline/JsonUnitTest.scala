package it.luca.pipeline

import argonaut._, Argonaut._
import org.apache.log4j.Logger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import scala.reflect.runtime.universe._

abstract class JsonUnitTest extends AnyFlatSpec with should.Matchers {

  private final val logger = Logger.getLogger(getClass)

  def className[T](implicit typeTag: TypeTag[T]): String = typeOf[T].typeSymbol.name.toString

  def toJsonString[T](tObject: T)(implicit encodeJson: EncodeJson[T], typeTag: TypeTag[T]): String = {

    val jsonString: String = tObject.jencode.spaces4
    logger.info(s"Json representation of input object of type ${className[T]}: \n\n$jsonString\n")
    jsonString
  }
}
