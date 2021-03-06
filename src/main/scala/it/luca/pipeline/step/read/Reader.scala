package it.luca.pipeline.step.read

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader[T <: ReadOptions] {

  def read(readOptions: T, sparkSession: SparkSession): DataFrame

}

sealed abstract class ReadOptions(val sourceType: String)

object ReadOptions extends DecodeJsonSubTypes[ReadOptions] {

  override protected val discriminatorField: String = JsonField.SourceType
  override protected val subclassesEncoders: Map[String, DecodeJson[_ <: ReadOptions]] = Map(
    JsonValue.Csv -> ReadCsvOptions.decodeJson,
    JsonValue.Hive -> ReadHiveTableOptions.decodeJson)
}

abstract class ReadFileOptions(override val sourceType: String, val path: String) extends ReadOptions(sourceType)

abstract class ReadTableOptions(override val sourceType: String, val tableName: Option[String], val sqlQuery: Option[String])
  extends ReadOptions(sourceType)
