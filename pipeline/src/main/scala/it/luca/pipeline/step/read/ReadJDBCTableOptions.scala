package it.luca.pipeline.step.read

import argonaut.DecodeJson
import it.luca.pipeline.step.common.{JDBCOptions, ReadTableOptions}

case class ReadJDBCTableOptions(override val sourceType: String,
                                override val dbName: String,
                                override val tableName: String,
                                override val query: Option[String],
                                jdbcOptions: JDBCOptions)

  extends ReadTableOptions(sourceType, dbName, tableName, query)

object ReadJDBCTableOptions {

  implicit def decodeJson: DecodeJson[ReadJDBCTableOptions] = DecodeJson.derive[ReadJDBCTableOptions]
}