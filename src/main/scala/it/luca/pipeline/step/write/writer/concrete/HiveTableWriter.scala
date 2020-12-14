package it.luca.pipeline.step.write.writer.concrete

import it.luca.pipeline.step.write.option.common.TableOptions
import it.luca.pipeline.step.write.option.concrete.WriteHiveTableOptions
import it.luca.pipeline.step.write.writer.common.Writer
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

object HiveTableWriter extends Writer[WriteHiveTableOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def write(dataFrame: DataFrame, writeOptions: WriteHiveTableOptions): Unit = {

    val dataframeWriter: DataFrameWriter[Row] = dataFrameWriter(dataFrame, writeOptions)
    val tableOptions: TableOptions = writeOptions.tableOptions
    val (dbName, tableName, saveMode): (String, String, String) = (tableOptions.dbName, tableOptions.tableName, writeOptions.saveOptions.saveMode)
    val fullTableName = s"$dbName.$tableName"

    val sparkSession = dataFrame.sparkSession
    createDbIfNotExists(sparkSession, writeOptions)
    if (sparkSession.catalog.tableExists(dbName, tableName)) {

      // If provided table exists, just .insertInto
      logger.info(s"Hive table '$fullTableName' already exists. So, starting to insert data within it (using .insertInto) with saveMode $saveMode")
      dataframeWriter.insertInto(fullTableName)
    } else {

      // Otherwise, .saveAsTable according to provided (or not) HDFS path
      val (pathInfoStr, dataFrameWriterMaybeWithPath): (String, DataFrameWriter[Row]) = writeOptions.createTableOptions.tablePath match {
        case None => (s"default location of database $dbName", dataframeWriter)
        case Some(x) => (s"path $x", dataframeWriter.option("path", x))
      }

      logger.warn(s"Hive table '$fullTableName' does not exist. So, creating it now (using .saveAsTable) at '$pathInfoStr'")
      dataFrameWriterMaybeWithPath
        .mode(saveMode)
        .saveAsTable(fullTableName)
    }
  }

  private def createDbIfNotExists(sparkSession: SparkSession, writeHiveTableOptions: WriteHiveTableOptions): Unit = {

    val dbName: String = writeHiveTableOptions.tableOptions.dbName
    val createDbIfNotExists: Boolean = writeHiveTableOptions.tableOptions.createDbIfNotExists
      .getOrElse("false")
      .toBoolean

    if (createDbIfNotExists) {

      // If provided Hive db exists, nothing to worry
      if (sparkSession.catalog.databaseExists(dbName)) {
        logger.info(s"Hive db '$dbName' already exists. Thus, not creating it again")
      } else {

        // Otherwise, create it at provided location (if any) or at default db location
        val defaultCreateDbStatement = s"CREATE DATABASE IF NOT EXISTS $dbName"
        val (dbLocationInfo, createDbStatement): (String, String) = writeHiveTableOptions.createTableOptions.dbPath match {
          case None => ("default location (default value of property 'spark.sql.warehouse.dir')", defaultCreateDbStatement)
          case Some(x) => (s"custom location $x", s"$defaultCreateDbStatement LOCATION '$x'")
        }

        logger.warn(s"Hive db '$dbName' does not exist yet. Creating it now at $dbLocationInfo")
        sparkSession.sql(createDbStatement)
        logger.info(s"Successfully created Hive db '$dbName' at $dbLocationInfo")
      }
    } else {
      logger.warn(s"Create db option is unset. Thus, things will turn bad if Hive db '$dbName' does not exist")
    }
  }
}
