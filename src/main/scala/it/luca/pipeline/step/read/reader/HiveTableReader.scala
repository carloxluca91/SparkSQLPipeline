package it.luca.pipeline.step.read.reader

import it.luca.pipeline.step.read.option.ReadHiveTableOptions
import it.luca.pipeline.utils.JobProperties
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveTableReader extends Reader[ReadHiveTableOptions] {

  override def read(srcOptions: ReadHiveTableOptions, sparkSession: SparkSession, jobProperties: JobProperties): DataFrame = {

    val dbName = jobProperties.get(srcOptions.dbName)
    val tableName = jobProperties.get(srcOptions.tableName)
    sparkSession.table(s"$dbName.$tableName")
  }
}
