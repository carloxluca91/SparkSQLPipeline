package it.luca.pipeline.step.read

import it.luca.pipeline.json.DecodeJsonDerive

case class CsvDataframeSchema(dataframeId: String,
                              description: String,
                              columns: List[CsvColumnSpecification])

object CsvDataframeSchema extends DecodeJsonDerive[CsvDataframeSchema]
