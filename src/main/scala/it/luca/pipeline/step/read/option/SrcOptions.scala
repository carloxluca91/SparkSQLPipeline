package it.luca.pipeline.step.read.option

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}

abstract class SrcOptions(val sourceType: String)

object SrcOptions extends DecodeJsonSubTypes[SrcOptions] {

  implicit def decodeJson: DecodeJson[SrcOptions] = decodeSubTypes(JsonField.SourceType.label,
    JsonValue.CsvSource.value -> CsvSrcOptions.decodeJson,
    JsonValue.HiveSource.value -> HiveTableSrcOptions.decodeJson,
    JsonValue.JDBCSource.value -> JDBCTableSrcOptions.decodeJson)
}
