package it.luca.pipeline.jdbc

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

object JDBCUtils {

  private val log = Logger.getLogger(getClass)

  final def getSparkWriterJDBCOptions(jdbcUrl: String, jdbcDriver: String, jdbcUser: String, jdbcPassword: String,
                                      jdbcUseSSL: String): Map[String, String] = {

    Map("url" -> jdbcUrl,
      "driver" -> jdbcDriver,
      "user" -> jdbcUser,
      "password" -> jdbcPassword,
      "useSSL" -> jdbcUseSSL)
  }

  final def getConnection(jdbcUrl: String, jdbcDriver: String, jdbcUserName: String, jdbcPassWord: String, jdbcUseSSL: String): Connection = {

    Class.forName(jdbcDriver)

    val jdbcUrlConnectionStr = s"$jdbcUrl/?useSSL=$jdbcUseSSL"
    log.info(s"Attempting to connect to JDBC url $jdbcUrlConnectionStr with credentials ($jdbcUserName, $jdbcPassWord)")
    val connection = DriverManager.getConnection(jdbcUrlConnectionStr, jdbcUserName, jdbcPassWord)
    log.info(s"Successfully connected to JDBC url $jdbcUrlConnectionStr with credentials ($jdbcUserName, $jdbcPassWord)")
    connection
  }

  final def getExistingDatabases(connection: Connection): Seq[String] = {

    // Result set containing existing db names
    val resultSet: ResultSet = connection.getMetaData.getCatalogs
    val existingDatabases = ListBuffer.empty[String]
    while (resultSet.next) {
      existingDatabases ++ resultSet.getString("TABLE_CAT").toLowerCase
    }
    existingDatabases
  }

  final def createDbIfNotExists(dbName: String, connection: Connection, closeAfterCreation: Boolean): Unit = {

    val existingDatabases: Seq[String] = getExistingDatabases(connection)
    val doesNotExistYet = !existingDatabases.contains(dbName.toLowerCase)
    if (doesNotExistYet) {
      log.info(s"Database '$dbName' does not exist yet. Thus, creating it now")
      val createDbStatement = connection.createStatement
      createDbStatement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName.toLowerCase)
      log.info(s"Successfully created database '$dbName'")
    } else {
      log.warn(String.format("Database '%s' already exists. Thus, not creating it again", dbName))
    }

    if (closeAfterCreation) {
      connection.close()
      log.info("Successfully closed JDBC connection")
    } else {
      log.warn("Not closing JDBC connection. Remember to close it manually when necessary")
    }
  }

  final def createDbIfNotExists(dbName: String, connection: Connection): Unit = {
    createDbIfNotExists(dbName, connection, closeAfterCreation = true)
  }
}
