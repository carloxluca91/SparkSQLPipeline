package it.luca.spark.sql.extensions

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class SparkSessionExtensions(private val sparkSession: SparkSession) {

  private val log = Logger.getLogger(classOf[SparkSessionExtensions])

  def createDbIfNotExists(dbName: String, dbHDFSPath: Option[String]): Unit = {

    // If given db does not exist, create it
    if (!sparkSession.catalog.databaseExists(dbName.toLowerCase)) {

      val createDbCommonStatement = s"CREATE DATABASE IF NOT EXISTS $dbName"
      val (dbLocationInfo, createDbFinalStatement): (String, String) = dbHDFSPath
        .map(s => (s"custom HDFS location '$s'", s"$createDbCommonStatement LOCATION '$s'"))
        .getOrElse("default location", createDbCommonStatement)

      log.warn(s"Hive db '$dbName' does not exist yet. Creating it now at $dbLocationInfo with following statement: $createDbFinalStatement")
      sparkSession.sql(createDbFinalStatement)
      log.info(s"Successfully created Hive db '$dbName' at $dbLocationInfo")

    } else {

      // Otherwise, just notify that db already exists
      log.info(s"Hive db '$dbName' already exists. Thus, not creating it again")
    }
  }
}
