package it.luca.pipeline.step.write.option.common

import argonaut.DecodeJson

abstract class WriteTableOptions(override val destinationType: String,
                                 override val saveOptions: SaveOptions,
                                 val tableOptions: TableOptions)

  extends WriteOptions(destinationType, saveOptions)

case class TableOptions(dbName: String,
                        tableName: String,
                        createDbIfNotExists: Option[String])

object TableOptions {

  implicit def decodeJson: DecodeJson[TableOptions] = DecodeJson.derive[TableOptions]
}
