package it.luca.pipeline.step.write.option

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}

abstract class WriteOptions(val destinationType: String,
                            val saveMode: String,
                            val partitionBy: Option[List[String]],
                            val coalesce: Option[Int])

object WriteOptions extends DecodeJsonSubTypes[WriteOptions] {

  implicit def decodeJson: DecodeJson[WriteOptions] = decodeSubTypes(JsonField.DestinationType.label,
    JsonValue.HiveSourceOrDestination.value -> WriteHiveTableOptions.decodeJson,
    JsonValue.JDBCSourceOrDestination.value -> WriteJDBCTableOptions.decodeJson)
}
