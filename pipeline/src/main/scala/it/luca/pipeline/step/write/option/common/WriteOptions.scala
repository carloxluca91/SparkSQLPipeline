package it.luca.pipeline.step.write.option.common

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}
import it.luca.pipeline.step.write.option.concrete._

abstract class WriteOptions(val destinationType: String,
                            val saveOptions: SaveOptions)

object WriteOptions extends DecodeJsonSubTypes[WriteOptions] {

  override protected val discriminatorField: String = JsonField.DestinationType.label

  override protected val subclassesEncoders: Map[String, DecodeJson[_ <: WriteOptions]] = Map(
    JsonValue.Hive -> WriteHiveTableOptions.decodeJson)
    //JsonValue.JDBCSourceOrDestination.value -> WriteJDBCTableOptions.decodeJson)
}

case class SaveOptions(saveMode: String,
                       partitionBy: Option[List[String]],
                       coalesce: Option[Int])

object SaveOptions {

  implicit def decodeJson: DecodeJson[SaveOptions] = DecodeJson.derive[SaveOptions]
}
