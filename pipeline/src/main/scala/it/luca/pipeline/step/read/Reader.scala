package it.luca.pipeline.step.read

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader[T <: ReadOptions] {

  def read(readOptions: T, sparkSession: SparkSession): DataFrame

}
