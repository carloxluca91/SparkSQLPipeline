package it.luca.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

package object extensions {

  implicit def toDataFrameExtensions(dataFrame: DataFrame): DataFrameExtensions = new DataFrameExtensions(dataFrame)

  implicit def toSparkSessionExtensions(sparkSession: SparkSession): SparkSessionExtensions = new SparkSessionExtensions(sparkSession)

}
