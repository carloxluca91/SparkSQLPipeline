package it.luca.pipeline.utils

import argonaut._
import Argonaut._
import it.luca.pipeline.exception.JsonParsingException
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, DataTypes}

import scala.io.{BufferedSource, Source}

object Utils {

  private final val logger = Logger.getLogger(getClass)

  private final val sparkDataTypeMap: Map[String, DataType] = Map(

    "string" -> DataTypes.StringType,
    "int" -> DataTypes.IntegerType,
    "long" -> DataTypes.LongType,
    "double" -> DataTypes.DoubleType,
    "date" -> DataTypes.DateType,
    "timestamp" -> DataTypes.TimestampType
  )

  final def asSparkDataType(dataType: String): DataType = {

    if (!sparkDataTypeMap.contains(dataType)) {
      logger.warn(s"Datatype '$dataType' not defined. Returning default datatype (${DataTypes.StringType})")
    }

    sparkDataTypeMap.getOrElse(dataType, DataTypes.StringType)
  }

  @throws[JsonParsingException[T]]
  final def decodeJsonFile[T](jsonFilePath: String)(implicit decodeJson: DecodeJson[T]): T = {

    val tClassName: String = classOf[T].getSimpleName
    logger.info(s"Trying to parse content of json file $jsonFilePath as an object of type $tClassName")
    val bufferedSource: BufferedSource = Source.fromFile(jsonFilePath, "UTF-8")
    val pipelineJsonString = bufferedSource.getLines().mkString
    bufferedSource.close()

    pipelineJsonString.decodeOption[T] match {
      case None =>
        logger.error(s"Unable to parse provided json file ($jsonFilePath) as an object of type $tClassName")
        throw JsonParsingException[T](jsonFilePath)
      case Some(value) =>
        logger.error(s"Successfully parsed provided json file ($jsonFilePath) as an object of type $tClassName")
        value
    }
  }

  final def datasetSchema(dataFrame: DataFrame): String = s"\n\n${dataFrame.schema.treeString}"
}
