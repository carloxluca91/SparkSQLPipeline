package it.luca.pipeline.step.read.option

import argonaut.DecodeJson

case class HiveTableSrcOptions(override val sourceType: String,
                               override val dbName: String,
                               override val tableName: String)
  extends TableSrcOptions(sourceType, dbName, tableName)

object HiveTableSrcOptions {

  implicit def decodeJson: DecodeJson[HiveTableSrcOptions] = DecodeJson.derive[HiveTableSrcOptions]
}
