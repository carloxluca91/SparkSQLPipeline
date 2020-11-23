package it.luca.pipeline.exception

case class JsonFileParsingException(jsonFilePath: String, tClassName: String)
  extends Throwable(s"Unable to parse provide json file $jsonFilePath as an object of type $tClassName")
