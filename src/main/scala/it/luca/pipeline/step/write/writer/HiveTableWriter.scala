package it.luca.pipeline.step.write.writer

import it.luca.pipeline.step.write.option.WriteHiveTableOptions
import it.luca.pipeline.utils.JobProperties
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

object HiveTableWriter extends Writer[WriteHiveTableOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def write(dataFrame: DataFrame, writeOptions: WriteHiveTableOptions,
                     sparkSession: SparkSession, jobProperties: JobProperties): Unit = {

    val dataframeWriter: DataFrameWriter[Row] = dataFrameWriter(dataFrame, writeOptions)
    val (dbName, tableName, saveMode) = (jobProperties.get(writeOptions.dbName),
      jobProperties.get(writeOptions.tableName),
      jobProperties.get(writeOptions.saveMode))

    val fullTableName = s"$dbName.$tableName"
    createDbIfNotExists(sparkSession, jobProperties, writeOptions)

    // Check if provided table exists
    if (sparkSession.catalog.tableExists(dbName, tableName)) {

      // If so, just insertInto
      logger.info(s"Hive table $fullTableName already exists. So, starting to insert data within it using saveMode $saveMode")
      dataframeWriter.insertInto(fullTableName)

    } else {

      // Otherwise, saveAsTable according to provided (or not) HDFS path
      val (pathInfoStr, dataFrameWriterMaybeWithPath): (String, DataFrameWriter[Row]) = writeOptions.tablePath match {
        case None => (s"default location of database $dbName", dataframeWriter)
        case Some(x) =>
          val tablePath: String = jobProperties.get(x)
          (s"path $tablePath", dataframeWriter.option("path", tablePath))
      }

      logger.warn(s"Hive table $fullTableName does not exist. So, creating it now at $pathInfoStr")
      dataFrameWriterMaybeWithPath
        .mode(saveMode)
        .saveAsTable(fullTableName)
    }
  }

  private final def createDbIfNotExists(sparkSession: SparkSession, jobProperties: JobProperties, writeHiveTableOptions: WriteHiveTableOptions): Unit = {

    val dbName: String = jobProperties.get(writeHiveTableOptions.dbName)
    val createDbIfNotExists: Boolean = jobProperties.getOrElseAs(writeHiveTableOptions.createDbIfNotExists, true)
    if (createDbIfNotExists) {

      if (sparkSession.catalog.databaseExists(dbName)) {
        logger.info(s"Hive db $dbName already exists. Thus, not creating it again")
      } else {

        val defaultCreateDbStatement = s"CREATE DATABASE IF NOT EXISTS $dbName"
        val (dbLocationInfo, createDbStatement): (String, String) = writeHiveTableOptions.dbPath match {
          case None => ("default location (default value of property 'spark.sql.warehouse.dir')", defaultCreateDbStatement)
          case Some(x) =>
            val dbLocation: String = jobProperties.get(x)
            (s"custom location $dbLocation", s"$defaultCreateDbStatement LOCATION '$dbLocation'")
        }

        logger.warn(s"Hive db $dbName does not exist yet. Creating it now at $dbLocationInfo")
        sparkSession.sql(createDbStatement)
        logger.info(s"Successfully created Hive db $dbName at $dbLocationInfo")
      }
    } else {
      logger.warn(s"Create db option is unset. Thus, things will turn bad if Hive db $dbName does not exist")
    }
  }
}
