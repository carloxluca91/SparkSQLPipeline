package it.luca.pipeline.step.read

import argonaut.DecodeJson
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveTableReader extends Reader[ReadHiveTableOptions] {

  private val log = Logger.getLogger(getClass)

  override def read(readOptions: ReadHiveTableOptions, sparkSession: SparkSession): DataFrame = {

    val (dbName, tableName): (String, String) = (readOptions.dbName, readOptions.tableName)
    readOptions.query match {
      case None =>

        log.info(s"No query provided. Reading whole Hive table '$dbName.$tableName'")
        sparkSession.table(s"$dbName.$tableName")

      case Some(value) =>

        log.info(s"Attempting to execute query: $value")
        val dataFrame = sparkSession.sql(value)
        log.info(s"Successfully executed query: $value")
        dataFrame
    }
  }
}

case class ReadHiveTableOptions(override val sourceType: String, override val dbName: String, override val tableName: String,
                                override val query: Option[String])
  extends ReadTableOptions(sourceType, dbName, tableName, query)

object ReadHiveTableOptions {

  implicit def decodeJson: DecodeJson[ReadHiveTableOptions] = DecodeJson.derive[ReadHiveTableOptions]
}
