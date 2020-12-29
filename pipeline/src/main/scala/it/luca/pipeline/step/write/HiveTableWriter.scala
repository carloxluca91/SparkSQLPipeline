package it.luca.pipeline.step.write

import argonaut.DecodeJson
import it.luca.spark.sql.SparkSessionUtils
import it.luca.spark.sql.extensions._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrameWriter, Row}

object HiveTableWriter extends Writer[WriteHiveTableOptions] {

  private val log = Logger.getLogger(getClass)

  override protected def writeDataFrame(dataFrameWriter: DataFrameWriter[Row], writeOptions: WriteHiveTableOptions): Unit = {

    val tableOptions = writeOptions.tableOptions
    val (dbName, tableName, saveMode): (String, String, String) = (tableOptions.dbName, tableOptions.tableName, writeOptions.saveOptions.saveMode)
    val fullTableName = s"$dbName.$tableName"

    val sparkSession = SparkSessionUtils.getOrCreateWithHiveSupport
    sparkSession.createDbIfNotExists(dbName, writeOptions.createTableOptions.dbPath)
    if (sparkSession.catalog.tableExists(dbName, tableName)) {

      // If provided table exists, just .insertInto
      log.info(s"Hive table '$fullTableName' already exists. So, starting to insert data within it (using .insertInto) with saveMode $saveMode")
      dataFrameWriter
        .mode(saveMode)
        .insertInto(fullTableName)
    } else {

      // Otherwise, .saveAsTable according to provided (or not) HDFS path
      val (pathInfoStr, dataFrameWriterMaybeWithPath): (String, DataFrameWriter[Row]) = writeOptions.createTableOptions.tablePath match {
        case None => (s"default location of database '$dbName'", dataFrameWriter)
        case Some(value) => (s"provided path '$value'", dataFrameWriter.option("path", value))
      }

      log.warn(s"Hive table '$fullTableName' does not exist. So, creating it now (using .saveAsTable) at $pathInfoStr")
      dataFrameWriterMaybeWithPath
        .mode(saveMode)
        .saveAsTable(fullTableName)
    }
  }
}

case class WriteHiveTableOptions(override val destinationType: String, override val saveOptions: SaveOptions, override val tableOptions: TableOptions,
                                 createTableOptions: CreateTableOptions)
  extends WriteTableOptions(destinationType, saveOptions, tableOptions)

object WriteHiveTableOptions {

  implicit def decodeJson: DecodeJson[WriteHiveTableOptions] = DecodeJson.derive[WriteHiveTableOptions]
}

case class CreateTableOptions(dbPath: Option[String], tablePath: Option[String])

object CreateTableOptions {

  implicit def decodeJson: DecodeJson[CreateTableOptions] = DecodeJson.derive[CreateTableOptions]
}
