package it.luca.pipeline.step.read

import argonaut.DecodeJson
import it.luca.pipeline.exception.ReadHiveTableOptionsException
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveTableReader extends Reader[ReadHiveTableOptions] {

  private val log = Logger.getLogger(getClass)

  override def read(readOptions: ReadHiveTableOptions, sparkSession: SparkSession): DataFrame = {

    readOptions.sqlQuery match {
      case None =>

        readOptions.tableName match {
          case None => throw ReadHiveTableOptionsException()
          case Some(value) =>
            log.info(s"No SQL query provided. Thus, reading whole Hive table '$value'")
            sparkSession.table(value)
        }

      case Some(value) =>

        log.info(s"Detected following SQL query: $value. Trying to execute it")
        val dataFrame = sparkSession.sql(value)
        log.info(s"Successfully executed provided SQL query: $value")
        dataFrame
    }
  }
}

case class ReadHiveTableOptions(override val sourceType: String, override val tableName: Option[String], override val sqlQuery: Option[String])
  extends ReadTableOptions(sourceType, tableName, sqlQuery)

object ReadHiveTableOptions {

  implicit def decodeJson: DecodeJson[ReadHiveTableOptions] = DecodeJson.derive[ReadHiveTableOptions]
}
