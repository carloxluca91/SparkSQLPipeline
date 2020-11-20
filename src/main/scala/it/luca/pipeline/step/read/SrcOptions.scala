package it.luca.pipeline.step.read

import argonaut.DecodeJson
import it.luca.pipeline.json.{JsonDecodeSubTypes, JsonField}

abstract class SrcOptions(val sourceType: String)

object SrcOptions extends JsonDecodeSubTypes[SrcOptions] {

  implicit def SrcOptionsDecodeJson: DecodeJson[SrcOptions] = decodeSubTypes(JsonField.SourceType.label,
  "csv" -> DecodeJson.derive[CsvSrcOptions],
    "hive" -> DecodeJson.derive[HiveTableSrcOptions],
    "jdbc" -> DecodeJson.derive[JDBCTableSrcOptions])
}
