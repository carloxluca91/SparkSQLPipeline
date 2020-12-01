package it.luca.pipeline.step.write.writer

import it.luca.pipeline.jdbc.JDBCUtils
import it.luca.pipeline.json.JsonField
import it.luca.pipeline.step.write.common.Writer
import it.luca.pipeline.step.write.option.WriteJDBCTableOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object JDBCTableWriter extends Writer[WriteJDBCTableOptions] {

  private val logger = Logger.getLogger(getClass)

  override def write(dataFrame: DataFrame, writeOptions: WriteJDBCTableOptions): Unit = {

    val dbName = writeOptions.dbName
    val tableName = writeOptions.tableName
    val jdbcUrl = writeOptions.jdbcOptions.jdbcUrl
    val jdbcDriver = writeOptions.jdbcOptions.jdbcDriver
    val jdbcUser = writeOptions.jdbcOptions.jdbcUser
    val jdbcPassword = writeOptions.jdbcOptions.jdbcPassword
    val jdbcUseSSL = writeOptions.jdbcOptions.jdbcUseSSL.getOrElse("false")

    writeOptions.createDbIfNotExists match {
      case None => logger.warn(s"Option ${JsonField.CreateDbIfNotExists.label} not set. Thus, things won't go well if db '$dbName' does not exist")
      case Some(value) => if (value.toBoolean) {

        val jdbcConnection = JDBCUtils.getConnection(jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword, jdbcUseSSL)
        JDBCUtils.createDbIfNotExists(dbName, jdbcConnection)
      } else {

        logger.warn(s"Option ${JsonField.CreateDbIfNotExists.label} set to false. Thus, things won't go well if db '$dbName' does not exist")
      }
    }

    val sparkWriterJDBCOptions: Map[String, String] = JDBCUtils.getJDBCOptionsForSparkWriter(jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword, jdbcUseSSL)
    logger.info(s"Starting to save data into JDBC table '$dbName.$tableName' using saveMode ${writeOptions.saveMode}")

    dataFrameWriter(dataFrame, writeOptions)
      .format("jdbc")
      .options(sparkWriterJDBCOptions)
      .option("dbTable", s"$dbName.$tableName")
      .mode(writeOptions.saveMode)
      .save()

    logger.info(s"Successfully saved data into JDBC table '$dbName.$tableName' using saveMode ${writeOptions.saveMode}")
  }
}
