package it.luca.pipeline.step.read.reader

import it.luca.pipeline.step.read.option.ReadOptions
import it.luca.pipeline.utils.JobProperties
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader[T <: ReadOptions] {

  def read(srcOptions: T, sparkSession: SparkSession, jobProperties: JobProperties): DataFrame

}
