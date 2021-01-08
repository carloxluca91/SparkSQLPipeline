package it.luca.pipeline.step.write

import argonaut.DecodeJson
import it.luca.spark.sql.extensions._
import org.apache.spark.sql.DataFrame

object HiveTableWriter extends Writer[WriteHiveTableOptions] {

  override def write(dataFrame: DataFrame, writeOptions: WriteHiveTableOptions): Unit = {

    val tableOptions = writeOptions.tableOptions
    val (dbName, tableName): (String, String) = (tableOptions.dbName, tableOptions.tableName)

    // Retrieve DataFrame's SparkSession in order to create provided db (if necessary)
    val sparkSession = dataFrame.sparkSession
    val (dbPathOpt, tablePathOpt) = writeOptions.createTableOptions match {
      case Some(x) => (x.dbPath, x.tablePath)
      case None => (None, None)
    }
    sparkSession.createDbIfNotExists(dbName, dbPathOpt)

    // Save data
    dataFrame
      .coalesce(writeOptions.saveOptions.coalesce)
      .saveAsTableOrInsertInto(s"$dbName.$tableName",
        writeOptions.saveOptions.saveMode,
        partitionByOpt = writeOptions.saveOptions.partitionBy,
        tablePathOpt = tablePathOpt)
  }
}

case class WriteHiveTableOptions(override val destinationType: String, override val saveOptions: SaveOptions,
                                 override val tableOptions: TableOptions, createTableOptions: Option[CreateTableOptions])
  extends WriteTableOptions(destinationType, saveOptions, tableOptions)

object WriteHiveTableOptions {

  implicit def decodeJson: DecodeJson[WriteHiveTableOptions] = DecodeJson.derive[WriteHiveTableOptions]
}

case class CreateTableOptions(dbPath: Option[String], tablePath: Option[String])

object CreateTableOptions {

  implicit def decodeJson: DecodeJson[CreateTableOptions] = DecodeJson.derive[CreateTableOptions]
}
