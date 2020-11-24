package it.luca.pipeline.step.read.option

import argonaut.DecodeJson

case class ReadCsvOptions(override val sourceType: String,
                          override val path: String,
                          schemaFile: String,
                          separator: Option[String],
                          header: Option[String])
  extends ReadFileOptions(sourceType, path)

object ReadCsvOptions {

  implicit def decodeJson: DecodeJson[ReadCsvOptions] = DecodeJson.derive[ReadCsvOptions]
}
