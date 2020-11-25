package it.luca.pipeline.step.read.reader

import it.luca.pipeline.step.read.option.ReadOptions
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader[T <: ReadOptions] {

  def read(srcOptions: T, sparkSession: SparkSession): DataFrame

}
