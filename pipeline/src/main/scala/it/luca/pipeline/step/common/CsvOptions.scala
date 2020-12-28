package it.luca.pipeline.step.common

import argonaut.DecodeJson
import it.luca.spark.sql.utils.DataTypeUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StructField, StructType}

case class CsvOptions(schema: List[CsvColumnSpecification],
                      separator: Option[String],
                      header: Option[String]) {

  private val log = Logger.getLogger(classOf[CsvOptions])

  def toStructType: StructType = {

    log.info(s"Processing metadata for each of the ${schema.size} column(s)")
    val csvStructFields: Seq[StructField] = schema
      .map(c => StructField(c.name, DataTypeUtils.dataType(c.dataType), c.nullable))

    log.info(s"Successfully processed metadata for each of the ${schema.size} column(s)")
    StructType(csvStructFields)
  }
}

object CsvOptions {

  implicit def decodeJson: DecodeJson[CsvOptions] = DecodeJson.derive[CsvOptions]
}

case class CsvColumnSpecification(name: String,
                                  description: String,
                                  dataType: String,
                                  nullable: Boolean)

object CsvColumnSpecification {

  implicit def decodeJson: DecodeJson[CsvColumnSpecification] = DecodeJson.derive[CsvColumnSpecification]
}
