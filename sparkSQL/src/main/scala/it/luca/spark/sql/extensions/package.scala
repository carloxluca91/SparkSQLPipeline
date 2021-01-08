package it.luca.spark.sql

import org.apache.spark.sql.{DataFrame, DataFrameWriter, SparkSession}

package object extensions {

  implicit def toDfExtensions(dataFrame: DataFrame): DataFrameExtensions =
    new DataFrameExtensions(dataFrame)

  implicit def toDfWriterExtensions(dataFrameWriter: DataFrameWriter[_]): DataFrameWriterExtensions =
    new DataFrameWriterExtensions(dataFrameWriter)

  implicit def toSparkSessionExtensions(sparkSession: SparkSession): SparkSessionExtensions =
    new SparkSessionExtensions(sparkSession)

}
