package it.luca.pipeline.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes}

object Spark {

  private final val logger = Logger.getLogger(getClass)
  private final val sparkDataTypeMap: Map[String, DataType] = Map(

    "string" -> DataTypes.StringType,
    "int" -> DataTypes.IntegerType,
    "long" -> DataTypes.LongType,
    "double" -> DataTypes.DoubleType,
    "date" -> DataTypes.DateType,
    "timestamp" -> DataTypes.TimestampType
  )

  def getOrCreateSparkSession: SparkSession = {

    logger.info(s"Trying to initialize a ${classOf[SparkSession].getSimpleName}")

    val sparkSession = SparkSession.builder
      .enableHiveSupport
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate

    logger.info(s"Successfully initialized ${classOf[SparkSession].getSimpleName} " +
      s"for application '${sparkSession.sparkContext.appName}', " +
      s"applicationId = ${sparkSession.sparkContext.applicationId}, " +
      s"UI url = ${sparkSession.sparkContext.uiWebUrl}")
    sparkSession
  }

  final def asSparkDataType(dataType: String): DataType = {

    if (!sparkDataTypeMap.contains(dataType)) {
      logger.warn(s"Datatype '$dataType' not defined. Returning default datatype (${DataTypes.StringType})")
    }

    sparkDataTypeMap.getOrElse(dataType, DataTypes.StringType)
  }

  final def dataframeSchema(dataFrame: DataFrame): String = s"\n\n${dataFrame.schema.treeString}"
}
