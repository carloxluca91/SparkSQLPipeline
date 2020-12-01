package it.luca.pipeline.spark

import it.luca.pipeline.json.{JsonUtils, JsonValue}
import it.luca.pipeline.step.read.option.CsvDataframeSchema
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils {

  private final val logger = Logger.getLogger(getClass)
  private final val sparkDataTypeMap: Map[String, DataType] = Map(

    JsonValue.StringType.value -> DataTypes.StringType,
    JsonValue.IntType.value -> DataTypes.IntegerType,
    JsonValue.DateType.value -> DataTypes.DateType,
    JsonValue.TimestampType.value -> DataTypes.TimestampType
  )

  final def asSparkDataType(dataType: String): DataType = {

    if (!sparkDataTypeMap.contains(dataType)) {
      logger.warn(s"Datatype '$dataType' not defined. Returning default datatype (${DataTypes.StringType})")
    }

    sparkDataTypeMap.getOrElse(dataType, DataTypes.StringType)
  }

  final def dataframeSchema(dataFrame: DataFrame): String = s"\n\n${dataFrame.schema.treeString}"

  final def fromSchemaToStructType(schemaFilePath: String): StructType = {

    val csvDataframeSchema: CsvDataframeSchema = JsonUtils.decodeJsonFile[CsvDataframeSchema](schemaFilePath)
    logger.info(s"Processing metadata for each of the ${csvDataframeSchema.columns.size} column(s)")
    val csvStructFields: Seq[StructField] = csvDataframeSchema
      .columns
      .map(c => StructField(c.name, asSparkDataType(c.dataType), c.nullable))

    logger.info(s"Successfully processed metadata for each of the ${csvDataframeSchema.columns.size} column(s)")
    StructType(csvStructFields)
  }

  final def getOrCreateSparkSession: SparkSession = {

    logger.info(s"Trying to initialize a ${classOf[SparkSession].getSimpleName}")

    val sparkSession = SparkSession.builder
      .enableHiveSupport
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate

    logger.info(s"Successfully initialized a ${classOf[SparkSession].getSimpleName} " +
      s"for application '${sparkSession.sparkContext.appName}', " +
      s"applicationId = ${sparkSession.sparkContext.applicationId}, " +
      s"UI url = ${sparkSession.sparkContext.uiWebUrl}")
    sparkSession
  }
}
