package it.luca.pipeline.step.read

import argonaut.DecodeJson
import it.luca.pipeline.step.common.CsvOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvReader extends Reader[ReadCsvOptions] {

  private val log = Logger.getLogger(getClass)

  override def read(readOptions: ReadCsvOptions, sparkSession: SparkSession): DataFrame = {

    val csvPath: String = readOptions.path
    val (separator, header): (String, Boolean) = (readOptions.csvOptions.separator.getOrElse(","),
      readOptions.csvOptions.header.getOrElse("false").toBoolean)

    log.info(
      s"""
         |      Provided csv details:
         |      path = $csvPath,
         |      separator = '$separator',
         |      header = $header
         |      """.stripMargin)

    sparkSession.read
      .format("csv")
      .option("sep", separator)
      .option("header", header)
      .schema(readOptions.csvOptions.schemaAsStructType)
      .load(csvPath)
  }
}

case class ReadCsvOptions(override val sourceType: String, override val path: String, csvOptions: CsvOptions)
  extends ReadFileOptions(sourceType, path)

object ReadCsvOptions {

  implicit def decodeJson: DecodeJson[ReadCsvOptions] = DecodeJson.derive[ReadCsvOptions]
}