package it.luca.pipeline.step.common

import argonaut.DecodeJson

case class CsvOptions(schemaFile: String,
                      separator: Option[String],
                      header: Option[String])

object CsvOptions {

  implicit def decodeJson: DecodeJson[CsvOptions] = DecodeJson.derive[CsvOptions]
}
