package it.luca.pipeline.step.read.option

import argonaut.DecodeJson
import it.luca.pipeline.step.common.CsvOptions
import it.luca.pipeline.step.read.common.ReadFileOptions

case class ReadCsvOptions(override val sourceType: String,
                          override val path: String,
                          csvOptions: CsvOptions)
  extends ReadFileOptions(sourceType, path)

object ReadCsvOptions {

  implicit def decodeJson: DecodeJson[ReadCsvOptions] = DecodeJson.derive[ReadCsvOptions]
}

case class CsvColumnSpecification(name: String,
                                  description: String,
                                  dataType: String,
                                  nullable: Boolean)

object CsvColumnSpecification {

  implicit def decodeJson: DecodeJson[CsvColumnSpecification] = DecodeJson.derive[CsvColumnSpecification]
}

case class CsvDataframeSchema(dataframeId: String,
                              description: String,
                              columns: List[CsvColumnSpecification])

object CsvDataframeSchema {

  implicit def decodeJson: DecodeJson[CsvDataframeSchema] = DecodeJson.derive[CsvDataframeSchema]
}