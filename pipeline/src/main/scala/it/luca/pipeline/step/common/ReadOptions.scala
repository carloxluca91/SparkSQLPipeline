package it.luca.pipeline.step.common

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}
import it.luca.pipeline.step.read.{ReadCsvOptions, ReadHiveTableOptions}

abstract class ReadOptions(val sourceType: String)

object ReadOptions extends DecodeJsonSubTypes[ReadOptions] {

  override protected val discriminatorField: String = JsonField.SourceType.label
  override protected val subclassesEncoders: Map[String, DecodeJson[_ <: ReadOptions]] = Map(
    JsonValue.Csv -> ReadCsvOptions.decodeJson,
    JsonValue.Hive -> ReadHiveTableOptions.decodeJson)
  //JsonValue.JDBCSourceOrDestination.value -> ReadJDBCTableOptions.decodeJson)
}

abstract class ReadFileOptions(override val sourceType: String,
                               val path: String)
  extends ReadOptions(sourceType)

abstract class ReadTableOptions(override val sourceType: String,
                                val dbName: String,
                                val tableName: String,
                                val query: Option[String])
  extends ReadOptions(sourceType)