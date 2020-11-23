package it.luca.pipeline.exception

case class JsonStringParsingException(tClassName: String)
  extends Throwable(s"Unable to parse provide json string as an object of type $tClassName")
