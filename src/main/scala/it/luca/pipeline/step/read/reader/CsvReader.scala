package it.luca.pipeline.step.read.reader

import it.luca.pipeline.spark.SparkUtils
import it.luca.pipeline.step.read.common.Reader
import it.luca.pipeline.step.read.option.ReadCsvOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvReader extends Reader[ReadCsvOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def read(srcOptions: ReadCsvOptions, sparkSession: SparkSession): DataFrame = {

    val (csvPath, csvSchemaFilePath): (String, String) = (srcOptions.path, srcOptions.schemaFile)
    val (separator, header): (String, Boolean) = (srcOptions.separator.getOrElse(","), srcOptions.header.getOrElse("false").toBoolean)
    logger.info(s"Provided csv details:\n\n" +
      s"  path = $csvPath,\n" +
      s"  schemaFile = $csvSchemaFilePath,\n" +
      s"  separator = '$separator',\n" +
      s"  header = $header\n")

    sparkSession.read
      .format("csv")
      .option("sep", separator)
      .option("header", header)
      .schema(SparkUtils.fromSchemaToStructType(csvSchemaFilePath))
      .load(csvPath)
  }
}
