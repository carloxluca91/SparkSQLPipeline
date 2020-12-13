package it.luca.pipeline.step.write.option

import argonaut.DecodeJson
import it.luca.pipeline.step.common.JDBCOptions
import it.luca.pipeline.step.write.common.WriteTableOptions

case class WriteJDBCTableOptions(override val destinationType: String,
                                 override val dbName: String,
                                 override val tableName: String,
                                 override val createDbIfNotExists: Option[String],
                                 override val saveMode: String,
                                 jdbcOptions: JDBCOptions,
                                 override val partitionBy: Option[List[String]],
                                 override val coalesce: Option[Int])

  extends WriteTableOptions(destinationType, dbName, tableName, createDbIfNotExists, saveMode, partitionBy, coalesce)

object WriteJDBCTableOptions {

  implicit def decodeJson: DecodeJson[WriteJDBCTableOptions] = DecodeJson.derive[WriteJDBCTableOptions]
}
