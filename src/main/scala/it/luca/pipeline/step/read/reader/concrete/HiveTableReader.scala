package it.luca.pipeline.step.read.reader.concrete

import it.luca.pipeline.step.read.option.concrete.ReadHiveTableOptions
import it.luca.pipeline.step.read.reader.common.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveTableReader extends Reader[ReadHiveTableOptions] {

  override def read(srcOptions: ReadHiveTableOptions, sparkSession: SparkSession): DataFrame = {

    val (dbName, tableName): (String, String) = (srcOptions.dbName, srcOptions.tableName)
    sparkSession.table(s"$dbName.$tableName")
  }
}
