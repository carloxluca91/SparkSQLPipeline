package it.luca.pipeline.step.common

import it.luca.pipeline.step.read.SrcOptions
import it.luca.pipeline.utils.JobProperties
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader[T <: SrcOptions] {

  def read(srcOptions: T, sparkSession: SparkSession, jobProperties: JobProperties): DataFrame

}
