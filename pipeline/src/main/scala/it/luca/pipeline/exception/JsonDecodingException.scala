package it.luca.pipeline.exception

import argonaut._, Argonaut._
import scala.reflect.runtime.universe._

case class JsonDecodingException(jsonString: String, tClassName: String)
  extends Throwable(s"Unable to parse provided json string as an object of type $tClassName. Provided string:\n\n$jsonString\n")

object JsonDecodingException {

  def apply[T](jsonString: String)(implicit typeTag: TypeTag[T]): JsonDecodingException = {

    val tClassName: String = typeOf[T].typeSymbol.name.toString
    val jsonPrettyString: String = jsonString.parseOption.get.spaces4
    new JsonDecodingException(jsonPrettyString, tClassName)
  }
}
