package it.luca.pipeline.step.write.option.concrete

import argonaut.DecodeJson
import it.luca.pipeline.step.common.JDBCOptions
import it.luca.pipeline.step.write.option.common.{SaveOptions, TableOptions, WriteTableOptions}

case class WriteJDBCTableOptions(override val destinationType: String,
                                 override val saveOptions: SaveOptions,
                                 override val tableOptions: TableOptions,
                                 jdbcOptions: JDBCOptions)

  extends WriteTableOptions(destinationType, saveOptions, tableOptions)

object WriteJDBCTableOptions {

  implicit def decodeJson: DecodeJson[WriteJDBCTableOptions] = DecodeJson.derive[WriteJDBCTableOptions]
}
