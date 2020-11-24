package it.luca.pipeline.step.read.option

import argonaut.DecodeJson

case class ReadHiveTableOptions(override val sourceType: String,
                                override val dbName: String,
                                override val tableName: String)
  extends ReadTableOptions(sourceType, dbName, tableName)

object ReadHiveTableOptions {

  implicit def decodeJson: DecodeJson[ReadHiveTableOptions] = DecodeJson.derive[ReadHiveTableOptions]
}
