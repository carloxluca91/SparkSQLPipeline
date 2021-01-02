package it.luca.spark.sql.extensions

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class SparkSessionExtensions(private val sparkSession: SparkSession) {

  private val log = Logger.getLogger(classOf[SparkSessionExtensions])

  def createDbIfNotExists(dbName: String, dbHDFSPath: Option[String]): Unit = {

    // If given db does not exist, create it
    if (!sparkSession.catalog.databaseExists(dbName.toLowerCase)) {

      val (dbLocationInfo, dbHDFSLocation): (String, String) = dbHDFSPath match {
        case None => ("default location", "")
        case Some(value) => (s"custom location '$value'", s"LOCATION '$value'")
      }

      val createDbStatement = s"CREATE DATABASE IF NOT EXISTS $dbName $dbHDFSLocation"
      log.warn(s"Hive db '$dbName' does not exist yet. Creating it now at $dbLocationInfo with following statement: $createDbStatement")
      sparkSession.sql(createDbStatement)
      log.info(s"Successfully created Hive db '$dbName' at $dbLocationInfo")

    } else {

      // Otherwise, just notify that db already exists
      log.info(s"Hive db '$dbName' already exists. Thus, not creating it again")
    }
  }
}
