package it.luca.pipeline.step.read.reader

import it.luca.pipeline.step.read.option.SrcOptions
import it.luca.pipeline.utils.JobProperties
import org.apache.spark.sql.{DataFrame, SparkSession}

trait AbstractReader[T <: SrcOptions] {

  def read(srcOptions: T, sparkSession: SparkSession, jobProperties: JobProperties): DataFrame

}
