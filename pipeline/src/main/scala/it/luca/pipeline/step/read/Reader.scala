package it.luca.pipeline.step.read

import it.luca.pipeline.step.common.ReadOptions
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader[T <: ReadOptions] {

  def read(readOptions: T, sparkSession: SparkSession): DataFrame

}
