package it.luca.spark.sql.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DataType, DataTypes}

object DataTypeUtils {

  private val logger = Logger.getLogger(getClass)
  private val dataTypeMap: Map[String, DataType] = Map(

    "string" -> DataTypes.StringType,
    "int" -> DataTypes.IntegerType,
    "double" -> DataTypes.DoubleType,
    "float" -> DataTypes.FloatType,
    "long" -> DataTypes.LongType,
    "date" -> DataTypes.DateType,
    "timestamp" -> DataTypes.TimestampType
  )

  def dataType(dataType: String): DataType = {

    if (!dataTypeMap.contains(dataType)) {
      logger.warn(s"Datatype '$dataType' not defined. Returning default datatype (${DataTypes.StringType})")
    }

    dataTypeMap.getOrElse(dataType, DataTypes.StringType)
  }
}
