package it.luca.pipeline.exception

case class UnExistingKeyException(key: String)
  extends Exception(s"Key '$key' does not exist within application .properties file")