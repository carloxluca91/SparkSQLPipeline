package it.luca.pipeline.step.write

import argonaut.DecodeJson
import it.luca.pipeline.jdbc.JDBCUtils
import it.luca.pipeline.json.JsonField
import it.luca.pipeline.step.common.JDBCOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrameWriter, Row}

object JDBCTableWriter extends Writer[WriteJDBCTableOptions] {

  private val log = Logger.getLogger(getClass)
  override protected def writeDataFrame(dataFrameWriter: DataFrameWriter[Row], writeOptions: WriteJDBCTableOptions): Unit = {

    val dbName = writeOptions.tableOptions.dbName
    val tableName = writeOptions.tableOptions.tableName
    val jdbcUrl = writeOptions.jdbcOptions.jdbcUrl
    val jdbcDriver = writeOptions.jdbcOptions.jdbcDriver
    val jdbcUser = writeOptions.jdbcOptions.jdbcUser
    val jdbcPassword = writeOptions.jdbcOptions.jdbcPassword
    val jdbcUseSSL = writeOptions.jdbcOptions.jdbcUseSSL.getOrElse("false")

    writeOptions.tableOptions.createDbIfNotExists match {
      case None => log.warn(s"Option ${JsonField.CreateDbIfNotExists.label} not set. Thus, things won't go well if db '$dbName' does not exist")
      case Some(value) => if (value.toBoolean) {

        val jdbcConnection = JDBCUtils.getConnection(jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword, jdbcUseSSL)
        JDBCUtils.createDbIfNotExists(dbName, jdbcConnection)
      } else {

        log.warn(s"Option ${JsonField.CreateDbIfNotExists.label} set to false. Thus, things won't go well if db '$dbName' does not exist")
      }
    }

    val sparkWriterJDBCOptions: Map[String, String] = JDBCUtils.getSparkWriterJDBCOptions(jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword, jdbcUseSSL)
    val saveMode: String = writeOptions.saveOptions.saveMode
    log.info(s"Starting to save data into JDBC table '$dbName.$tableName' using saveMode $saveMode")

    dataFrameWriter
      .format("jdbc")
      .options(sparkWriterJDBCOptions)
      .option("dbTable", s"$dbName.$tableName")
      .mode(saveMode)
      .save()

    log.info(s"Successfully saved data into JDBC table '$dbName.$tableName' using saveMode $saveMode")
  }
}

case class WriteJDBCTableOptions(override val destinationType: String, override val saveOptions: SaveOptions, override val tableOptions: TableOptions,
                                 jdbcOptions: JDBCOptions)
  extends WriteTableOptions(destinationType, saveOptions, tableOptions)

object WriteJDBCTableOptions {

  implicit def decodeJson: DecodeJson[WriteJDBCTableOptions] = DecodeJson.derive[WriteJDBCTableOptions]
}