package it.luca.pipeline.step.write.option.common

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}
import it.luca.pipeline.step.write.option.concrete._

abstract class WriteOptions(val destinationType: String,
                            val saveOptions: SaveOptions)

object WriteOptions extends DecodeJsonSubTypes[WriteOptions] {

  implicit def decodeJson: DecodeJson[WriteOptions] = decodeSubTypes(JsonField.DestinationType.label,
    JsonValue.HiveSourceOrDestination.value -> WriteHiveTableOptions.decodeJson,
    JsonValue.JDBCSourceOrDestination.value -> WriteJDBCTableOptions.decodeJson)
}

case class SaveOptions(saveMode: String,
                       partitionBy: Option[List[String]],
                       coalesce: Option[Int])

object SaveOptions {

  implicit def decodeJson: DecodeJson[SaveOptions] = DecodeJson.derive[SaveOptions]
}
