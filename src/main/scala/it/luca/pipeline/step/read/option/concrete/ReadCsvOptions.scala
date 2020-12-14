package it.luca.pipeline.step.read.option.concrete

import argonaut.DecodeJson
import it.luca.pipeline.step.common.CsvOptions
import it.luca.pipeline.step.read.option.common.ReadFileOptions
import it.luca.spark.sql.utils.DataTypeUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StructField, StructType}

case class ReadCsvOptions(override val sourceType: String,
                          override val path: String,
                          csvOptions: CsvOptions)
  extends ReadFileOptions(sourceType, path)

object ReadCsvOptions {

  implicit def decodeJson: DecodeJson[ReadCsvOptions] = DecodeJson.derive[ReadCsvOptions]
}

case class CsvColumnSpecification(name: String,
                                  description: String,
                                  dataType: String,
                                  nullable: Boolean)

object CsvColumnSpecification {

  implicit def decodeJson: DecodeJson[CsvColumnSpecification] = DecodeJson.derive[CsvColumnSpecification]
}

case class CsvSchema(description: String,
                     columns: List[CsvColumnSpecification])

object CsvSchema {

  implicit def decodeJson: DecodeJson[CsvSchema] = DecodeJson.derive[CsvSchema]

  private val logger = Logger.getLogger(classOf[CsvSchema])

  def toStructType(csvSchema: CsvSchema): StructType = {

    logger.info(s"Processing metadata for each of the ${csvSchema.columns.size} column(s)")
    val csvStructFields: Seq[StructField] = csvSchema
      .columns
      .map(c => StructField(c.name, DataTypeUtils.dataType(c.dataType), c.nullable))

    logger.info(s"Successfully processed metadata for each of the ${csvSchema.columns.size} column(s)")
    StructType(csvStructFields)
  }
}