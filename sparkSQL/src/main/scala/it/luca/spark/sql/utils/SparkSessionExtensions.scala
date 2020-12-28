package it.luca.spark.sql.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class SparkSessionExtensions(private val sparkSession: SparkSession) {

  private val log = Logger.getLogger(classOf[SparkSessionExtensions])

  def createDbIfNotExists(dbName: String, dbHDFSPath: Option[String]): Unit = {

    // If given db does not exist, create it
    if (sparkSession.catalog.databaseExists(dbName.toLowerCase)) {

      log.warn(s"Hive db '$dbName' does not exist yet. Creating it now")
      val (dbLocationInfo, dbHDFSLocation) :(String, String) = dbHDFSPath match {
        case None => ("default location", "")
        case Some(value) => (s"custom location '$value'", s"LOCATION '$value'")
      }

      val createDbStatement = s"CREATE DATABASE IF NOT EXISTS $dbName $dbHDFSLocation"
      log.info(s"Attempting to create Hive db '$dbName' at $dbLocationInfo with following statement: $createDbStatement")
      sparkSession.sql(createDbStatement)
      log.info(s"Successfully created Hive db '$dbName' at $dbLocationInfo")

    } else {

      // Otherwise, just notify that db already exists
      log.info(s"Hive db '$dbName' already exists. Thus, not creating it again")
    }
  }
}
