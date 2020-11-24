package it.luca.pipeline

import it.luca.pipeline.data.LogRecord
import it.luca.pipeline.utils.{JDBCUtils, JobProperties, JsonUtils, SparkUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object PipelineRunner {

  private final val logger = Logger.getLogger(getClass)

  private final def getPipelineFilePathOpt(pipelineName: String, sparkSession: SparkSession, jobProperties: JobProperties): Option[String] = {

    val hiveDefaultDbName = jobProperties.get("hive.database.default.name")
    val existsDefaultHiveDb = sparkSession.catalog.databaseExists(hiveDefaultDbName.toLowerCase)
    if (!existsDefaultHiveDb) {

      // If Hive default Db does not exist, run INITIAL_LOAD pipeline
      val hiveDbWarningMsg = s"Hive db '$hiveDefaultDbName' does not exist"
      val warningMsg = if (pipelineName equalsIgnoreCase "INITIAL_LOAD") hiveDbWarningMsg
      else s"$hiveDbWarningMsg. Thus, running 'INITIAL_LOAD' pipeline instead of '$pipelineName'"

      logger.warn(warningMsg)
      Some(jobProperties.get("initialLoad.file.path"))
    } else {

      // Otherwise, run provided pipeline (or at least try to ;)
      val pipelineInfoTableName = jobProperties.get("hive.table.pipelineInfo.name")
      val pipelineInfoTableNameFull = jobProperties.get("hive.table.pipelineInfo.fullName")
      val existsPipelineInfoTable: Boolean = sparkSession.catalog.tableExists(pipelineInfoTableName, hiveDefaultDbName)
      val isPipelineInfoTableNotEmpty: Boolean = existsPipelineInfoTable && sparkSession.table(pipelineInfoTableNameFull).count > 0
      if (existsPipelineInfoTable & isPipelineInfoTableNotEmpty) {

        // If everything seems to be ok with pipelineInfoTable, look up for provided pipeline name
        logger.info(s"Table '$pipelineInfoTableNameFull' exists and it's not empty. " +
          s"Thus, looking for information on pipeline '$pipelineName' within it")

        val sqlQuery = s"SELECT file_name FROM $pipelineInfoTableNameFull WHERE trim(lower(pipeline_name)) = '${pipelineName.toLowerCase}'"
        val pipelineInfoRows: Array[Row] = sparkSession.sql(sqlQuery).collect()

        // If no information is found, sorry ;)
        if (pipelineInfoRows.isEmpty) {
          None
        } else {

          val pipelineJsonFilePath: String = pipelineInfoRows(0).getAs(0)
          logger.info(s"Json file for pipeline '$pipelineName': '$pipelineJsonFilePath'")
          Some(pipelineJsonFilePath)
        }
      } else {

        // Otherwise, if something seems to be wrong with pipelineInfo, rerun INITIAL_LOAD
        val pipelineInfoTableWarningMsg = if (existsPipelineInfoTable) s"Table '$pipelineInfoTableNameFull' is empty" else
          s"Table '$pipelineInfoTableNameFull' does not exist"

        val warningMsg = if (pipelineName equalsIgnoreCase "INITIAL_LOAD") pipelineInfoTableWarningMsg else
          s"$pipelineInfoTableWarningMsg. Thus, running 'INITIAL_LOAD' pipeline instead of '$pipelineName'"
        logger.warn(warningMsg)
        Some(jobProperties.get("initialLoad.file.path"))
      }
    }
  }

  private def logToJDBC(logRecords: Seq[LogRecord], sparkSession: SparkSession, jobProperties: JobProperties): Unit = {

    import sparkSession.implicits._

    val logRecordDf: DataFrame = logRecords.toDF

    logger.info(s"Successfully turned list of ${logRecords.size} object(s) " +
      s"of type ${classOf[LogRecord].getSimpleName} " +
      s"into a ${classOf[DataFrame].getSimpleName}")

    // Extract relevant info for creating JDBC connection
    val jdbcLoggingDatabase = jobProperties.get("jdbc.database.default.name")
    val jdbcUrl = jobProperties.get("jdbc.default.url")
    val jdbcDriver = jobProperties.get("jdbc.default.driver.className")
    val jdbcUserName = jobProperties.get("jdbc.default.userName")
    val jdbcPassWord = jobProperties.get("jdbc.default.passWord")
    val jdbcUseSSL = jobProperties.get("jdbc.default.useSSL")

    // Create database hosting logTable if necessary
    val connection: java.sql.Connection = JDBCUtils.getConnection(jdbcUrl, jdbcDriver, jdbcUserName, jdbcPassWord, jdbcUseSSL)
    JDBCUtils.createDbIfNotExists(jdbcLoggingDatabase, connection)

    // Setup DataFrameWriter JDBC options
    val logTableFullName = jobProperties.get("jdbc.table.logging.table.fullName")
    val jdbcOptions: Map[String, String] = Map(

      "url" ->jdbcUrl,
      "driver" -> jdbcDriver,
      "user" -> jdbcUserName,
      "password" -> jdbcPassWord,
      "useSSL" -> jdbcUseSSL
    )

    logger.info(s"Logging dataframe schema: ${SparkUtils.dataframeSchema(logRecordDf)}")
    logRecordDf.coalesce(1)
      .write
      .format("jdbc")
      .options(jdbcOptions)
      .option("dbTable", logTableFullName)
      .mode(SaveMode.Append)
      .save

    logger.info(s"Successfully inserted ${logRecords.size} logging record(s) within table '$logTableFullName'")
  }

  def run(pipelineName: String, propertiesFile: String): Unit = {

    lazy val sparkSession: SparkSession = SparkUtils.getOrCreateSparkSession
    val jobProperties: JobProperties = JobProperties(propertiesFile)
    val pipelineFilePathOpt: Option[String] = getPipelineFilePathOpt(pipelineName, sparkSession, jobProperties)
    if (pipelineFilePathOpt.nonEmpty) {

      // Try to parse provided json file as a Pipeline and run it
      val pipeline: Pipeline = JsonUtils.decodeJsonFile[Pipeline](pipelineFilePathOpt.get)
      val (pipelineFullyExecuted, logRecords) : (Boolean, Seq[LogRecord]) = pipeline.run(sparkSession, jobProperties)
      logToJDBC(logRecords, sparkSession, jobProperties)
      if (pipelineFullyExecuted) {
        logger.info(s"Successfully executed whole pipeline '$pipelineName'")

      } else {
        logger.warn(s"Unable to fully execute pipeline '$pipelineName'")
      }
    } else {
      logger.warn(s"Unable to retrieve any record(s) related to pipeline '$pipelineName'. Thus, nothing will be triggered")
    }
  }
}
