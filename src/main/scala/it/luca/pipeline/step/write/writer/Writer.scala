package it.luca.pipeline.step.write.writer

import it.luca.pipeline.step.write.option.WriteOptions
import it.luca.pipeline.utils.JobProperties
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Writer[T <: WriteOptions] {

  def write(dataFrame: DataFrame, writeOptions: T, sparkSession: SparkSession, jobProperties: JobProperties): Unit

}
