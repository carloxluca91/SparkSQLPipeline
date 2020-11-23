package it.luca.pipeline.step.read.option

import argonaut.DecodeJson

case class CsvColumnSpecification(name: String,
                                  description: String,
                                  dataType: String,
                                  nullable: Boolean)

object CsvColumnSpecification {

  implicit def decodeJson: DecodeJson[CsvColumnSpecification] = DecodeJson.derive[CsvColumnSpecification]
}
