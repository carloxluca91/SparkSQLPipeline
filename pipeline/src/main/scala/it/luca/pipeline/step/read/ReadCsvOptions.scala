package it.luca.pipeline.step.read

import argonaut.DecodeJson
import it.luca.pipeline.step.common.CsvOptions

case class ReadCsvOptions(override val sourceType: String,
                          override val path: String,
                          csvOptions: CsvOptions)
  extends ReadFileOptions(sourceType, path)

object ReadCsvOptions {

  implicit def decodeJson: DecodeJson[ReadCsvOptions] = DecodeJson.derive[ReadCsvOptions]
}