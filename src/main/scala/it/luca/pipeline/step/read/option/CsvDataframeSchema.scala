package it.luca.pipeline.step.read.option

import argonaut.DecodeJson

case class CsvDataframeSchema(dataframeId: String,
                              description: String,
                              columns: List[CsvColumnSpecification])

object CsvDataframeSchema {

  implicit def decodeJson: DecodeJson[CsvDataframeSchema] = DecodeJson.derive[CsvDataframeSchema]
}
