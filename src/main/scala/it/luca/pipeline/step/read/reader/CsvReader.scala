package it.luca.pipeline.step.read.reader

import it.luca.pipeline.step.read.option.{CsvDataframeSchema, CsvSrcOptions}
import it.luca.pipeline.utils.{JobProperties, Json, Spark}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvReader extends AbstractReader[CsvSrcOptions] {

  private final val logger = Logger.getLogger(getClass)

  private final def fromSchemaToStructType(schemaFilePath: String): StructType = {

    val csvDataframeSchema: CsvDataframeSchema = Json.decodeJsonFile[CsvDataframeSchema](schemaFilePath)
    logger.info(s"Processing metadata for each of the ${csvDataframeSchema.columns.size} columns")
    val csvStructFields: Seq[StructField] = csvDataframeSchema
      .columns
      .map(c => {
        StructField(c.name, Spark.asSparkDataType(c.dataType), c.nullable)
      })

    logger.info(s"Successfully processed metadata for each of the ${csvDataframeSchema.columns.size} columns")
    StructType(csvStructFields)
  }

  override def read(srcOptions: CsvSrcOptions, sparkSession: SparkSession, jobProperties: JobProperties): DataFrame = {

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
      .schema(fromSchemaToStructType(csvSchemaFilePath))
      .load(csvPath)
  }
}
