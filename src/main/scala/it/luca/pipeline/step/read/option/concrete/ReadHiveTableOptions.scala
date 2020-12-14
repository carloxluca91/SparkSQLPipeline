package it.luca.pipeline.step.read.option.concrete

import argonaut.DecodeJson
import it.luca.pipeline.step.read.option.common.ReadTableOptions

case class ReadHiveTableOptions(override val sourceType: String,
                                override val dbName: String,
                                override val tableName: String)
  extends ReadTableOptions(sourceType, dbName, tableName)

object ReadHiveTableOptions {

  implicit def decodeJson: DecodeJson[ReadHiveTableOptions] = DecodeJson.derive[ReadHiveTableOptions]
}
