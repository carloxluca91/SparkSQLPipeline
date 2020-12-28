package it.luca.pipeline.step.write.option.concrete

import argonaut.DecodeJson
import it.luca.pipeline.step.write.option.common.{SaveOptions, TableOptions, WriteTableOptions}

case class WriteHiveTableOptions(override val destinationType: String,
                                 override val saveOptions: SaveOptions,
                                 override val tableOptions: TableOptions,
                                 createTableOptions: CreateTableOptions)

  extends WriteTableOptions(destinationType, saveOptions, tableOptions)

object WriteHiveTableOptions {

  implicit def decodeJson: DecodeJson[WriteHiveTableOptions] = DecodeJson.derive[WriteHiveTableOptions]
}

case class CreateTableOptions(dbPath: Option[String],
                              tablePath: Option[String])

object CreateTableOptions {

  implicit def decodeJson: DecodeJson[CreateTableOptions] = DecodeJson.derive[CreateTableOptions]
}
