package it.luca.pipeline.step.read.common

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader[T <: ReadOptions] {

  def read(srcOptions: T, sparkSession: SparkSession): DataFrame

}
