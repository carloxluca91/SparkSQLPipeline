package it.luca.pipeline.exception

import scala.reflect.runtime.universe._

case class JsonDecodingException(decodingMsg: String, tClassName: String)
  extends Throwable(s"Unable to parse provided json string as an object of type $tClassName. JSON decoding message: $decodingMsg")

object JsonDecodingException {

  def apply[T](decodingMsg: String)(implicit typeTag: TypeTag[T]): JsonDecodingException = {

    val tClassName: String = typeOf[T].typeSymbol.name.toString
    new JsonDecodingException(decodingMsg, tClassName)
  }
}
