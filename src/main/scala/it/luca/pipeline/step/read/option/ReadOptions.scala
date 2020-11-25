package it.luca.pipeline.step.read.option

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}

abstract class ReadOptions(val sourceType: String)

object ReadOptions extends DecodeJsonSubTypes[ReadOptions] {

  implicit def decodeJson: DecodeJson[ReadOptions] = decodeSubTypes(JsonField.SourceType.label,
    JsonValue.CsvSourceOrDestination.value -> ReadCsvOptions.decodeJson,
    JsonValue.HiveSourceOrDestination.value -> ReadHiveTableOptions.decodeJson,
    JsonValue.JDBCSourceOrDestination.value -> ReadJDBCTableOptions.decodeJson)
}