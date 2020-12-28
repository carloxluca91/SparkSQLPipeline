package it.luca.pipeline.step.read

import argonaut.DecodeJson
import it.luca.pipeline.step.common.ReadTableOptions

case class ReadHiveTableOptions(override val sourceType: String,
                                override val dbName: String,
                                override val tableName: String,
                                override val query: Option[String])
  extends ReadTableOptions(sourceType, dbName, tableName, query)

object ReadHiveTableOptions {

  implicit def decodeJson: DecodeJson[ReadHiveTableOptions] = DecodeJson.derive[ReadHiveTableOptions]
}
