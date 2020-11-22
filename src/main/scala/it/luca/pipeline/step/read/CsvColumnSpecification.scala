package it.luca.pipeline.step.read

import it.luca.pipeline.json.DecodeJsonDerive

case class CsvColumnSpecification(name: String,
                                  description: String,
                                  dataType: String,
                                  nullable: Boolean)

object CsvColumnSpecification extends DecodeJsonDerive[CsvColumnSpecification]
