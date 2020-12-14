package it.luca.pipeline.step.read.reader.common

import it.luca.pipeline.step.read.option.common.ReadOptions
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader[T <: ReadOptions] {

  def read(srcOptions: T, sparkSession: SparkSession): DataFrame

}
