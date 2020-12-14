package it.luca.pipeline.step.read.reader.concrete

import it.luca.pipeline.json.JsonUtils
import it.luca.pipeline.step.read.option.concrete.{CsvSchema, ReadCsvOptions}
import it.luca.pipeline.step.read.reader.common.Reader
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvReader extends Reader[ReadCsvOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def read(srcOptions: ReadCsvOptions, sparkSession: SparkSession): DataFrame = {

    val (csvPath, csvSchemaFilePath): (String, String) = (srcOptions.path, srcOptions.csvOptions.schemaFile)
    val (separator, header): (String, Boolean) = (srcOptions.csvOptions.separator.getOrElse(","),
      srcOptions.csvOptions.header.getOrElse("false").toBoolean)

    logger.info(s"Provided csv details: path = $csvPath, schemaFile = $csvSchemaFilePath, separator = '$separator', header = $header")

    sparkSession.read
      .format("csv")
      .option("sep", separator)
      .option("header", header)
      .schema(CsvSchema.toStructType(JsonUtils.decodeJsonFile[CsvSchema](csvSchemaFilePath)))
      .load(csvPath)
  }
}
