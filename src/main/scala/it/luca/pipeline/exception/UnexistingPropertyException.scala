package it.luca.pipeline.exception

case class UnexistingPropertyException(key: String)
  extends Throwable(s"Key '$key' does not exist within application .properties file")