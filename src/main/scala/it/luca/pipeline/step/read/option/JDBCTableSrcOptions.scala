package it.luca.pipeline.step.read.option

import argonaut.DecodeJson

case class JDBCTableSrcOptions(override val sourceType: String,
                               override val dbName: String,
                               override val tableName: String,
                               jdbcUrl: String,
                               jdbcDriver: String,
                               jdbcUser: String,
                               jdbcPassword: String,
                               jdbcUseSSL: Option[String])
  extends TableSrcOptions(sourceType, dbName, tableName)

object JDBCTableSrcOptions {

  implicit def decodeJson: DecodeJson[JDBCTableSrcOptions] = DecodeJson.derive[JDBCTableSrcOptions]
}