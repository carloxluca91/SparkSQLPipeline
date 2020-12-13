package it.luca.pipeline.step.read.option

import argonaut.DecodeJson
import it.luca.pipeline.step.common.JDBCOptions
import it.luca.pipeline.step.read.common.ReadTableOptions

case class ReadJDBCTableOptions(override val sourceType: String,
                                override val dbName: String,
                                override val tableName: String,
                                jdbcOptions: JDBCOptions)

  extends ReadTableOptions(sourceType, dbName, tableName)

object ReadJDBCTableOptions {

  implicit def decodeJson: DecodeJson[ReadJDBCTableOptions] = DecodeJson.derive[ReadJDBCTableOptions]
}