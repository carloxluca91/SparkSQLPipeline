package it.luca.pipeline.step.write.writer

import it.luca.pipeline.step.write.option.WriteHiveTableOptions
import it.luca.pipeline.utils.JobProperties
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveTableWriter extends Writer[WriteHiveTableOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def write(dataFrame: DataFrame, writeOptions: WriteHiveTableOptions,
                     sparkSession: SparkSession, jobProperties: JobProperties): Unit = {

  }
}
