package it.luca.pipeline.exception

case class JsonParsingException[T](jsonFilePath: String)
  extends Throwable(s"Unable to parse provide json file $jsonFilePath as an object of type ${classOf[T].getSimpleName}")
