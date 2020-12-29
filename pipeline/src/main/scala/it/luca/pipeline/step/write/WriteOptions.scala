package it.luca.pipeline.step.write

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}

sealed abstract class WriteOptions(val destinationType: String, val saveOptions: SaveOptions)

object WriteOptions extends DecodeJsonSubTypes[WriteOptions] {

  override protected val discriminatorField: String = JsonField.DestinationType.label

  override protected val subclassesEncoders: Map[String, DecodeJson[_ <: WriteOptions]] = Map(
    JsonValue.Hive -> WriteHiveTableOptions.decodeJson)
    //JsonValue.JDBCSourceOrDestination.value -> WriteJDBCTableOptions.decodeJson)
}

case class SaveOptions(saveMode: String, partitionBy: Option[List[String]], coalesce: Option[Int])

object SaveOptions {

  implicit def decodeJson: DecodeJson[SaveOptions] = DecodeJson.derive[SaveOptions]
}

abstract class WriteFileOptions(override val destinationType: String, override val saveOptions: SaveOptions, val path: String)
  extends WriteOptions(destinationType, saveOptions)

abstract class WriteTableOptions(override val destinationType: String, override val saveOptions: SaveOptions, val tableOptions: TableOptions)
  extends WriteOptions(destinationType, saveOptions)

case class TableOptions(dbName: String, tableName: String, createDbIfNotExists: Option[String])

object TableOptions {

  implicit def decodeJson: DecodeJson[TableOptions] = DecodeJson.derive[TableOptions]
}

