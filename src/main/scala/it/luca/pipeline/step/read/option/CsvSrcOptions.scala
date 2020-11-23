package it.luca.pipeline.step.read.option

import argonaut.DecodeJson

case class CsvSrcOptions(override val sourceType: String,
                         override val path: String,
                         schemaFile: String,
                         separator: Option[String],
                         header: Option[String])
  extends FileSrcOptions(sourceType, path)

object CsvSrcOptions {

  implicit def decodeJson: DecodeJson[CsvSrcOptions] = DecodeJson.derive[CsvSrcOptions]
}
