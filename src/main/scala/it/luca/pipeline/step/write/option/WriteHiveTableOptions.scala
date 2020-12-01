package it.luca.pipeline.step.write.option

import argonaut.DecodeJson
import it.luca.pipeline.step.write.common.WriteTableOptions

case class WriteHiveTableOptions(override val destinationType: String,
                                 override val dbName: String,
                                 override val tableName: String,
                                 override val createDbIfNotExists: Option[String],
                                 dbPath: Option[String],
                                 tablePath: Option[String],
                                 override val saveMode: String,
                                 override val partitionBy: Option[List[String]],
                                 override val coalesce: Option[Int])

  extends WriteTableOptions(destinationType, dbName, tableName, createDbIfNotExists, saveMode, partitionBy, coalesce)

object WriteHiveTableOptions {

  implicit def decodeJson: DecodeJson[WriteHiveTableOptions] = DecodeJson.derive[WriteHiveTableOptions]
}
