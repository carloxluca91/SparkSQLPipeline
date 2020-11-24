package it.luca.pipeline.step.read.reader

import it.luca.pipeline.step.read.option.ReadCsvOptions
import it.luca.pipeline.utils.{JobProperties, SparkUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvReader extends Reader[ReadCsvOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def read(srcOptions: ReadCsvOptions, sparkSession: SparkSession, jobProperties: JobProperties): DataFrame = {

    val csvPath: String = jobProperties.get(srcOptions.path)
    val csvSchemaFilePath: String = jobProperties.get(srcOptions.schemaFile)
    val separator: String = jobProperties.getOrElse(srcOptions.separator, ",")
    val header: Boolean = jobProperties.getOrElseAs(srcOptions.header, false)

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
