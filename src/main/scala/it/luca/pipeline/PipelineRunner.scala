package it.luca.pipeline

import it.luca.pipeline.spark.data.LogRecord
import it.luca.pipeline.jdbc.JDBCUtils
import it.luca.pipeline.json.JsonUtils
import it.luca.pipeline.spark.utils.SparkUtils
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object PipelineRunner {

  private val logger = Logger.getLogger(getClass)

  private def getPipelineFilePathOpt(pipelineName: String, sparkSession: SparkSession, jobProperties: PropertiesConfiguration): Option[String] = {

    val hiveDefaultDbName = jobProperties.getString("hive.database.default.name")
    val existsDefaultHiveDb = sparkSession.catalog.databaseExists(hiveDefaultDbName.toLowerCase)
    if (!existsDefaultHiveDb) {

      // If Hive default Db does not exist, run INITIAL_LOAD pipeline
      val hiveDbWarningMsg = s"Hive db '$hiveDefaultDbName' does not exist"
      val warningMsg = if (pipelineName equalsIgnoreCase "INITIAL_LOAD") hiveDbWarningMsg
      else s"$hiveDbWarningMsg. Thus, running 'INITIAL_LOAD' pipeline instead of '$pipelineName'"

      logger.warn(warningMsg)
      Some(jobProperties.getString("initialLoad.file.path"))
    } else {

      // Otherwise, run provided pipeline (or at least try to ;)
      val pipelineInfoTableName = jobProperties.getString("hive.table.pipelineInfo.name")
      val pipelineInfoTableNameFull = jobProperties.getString("hive.table.pipelineInfo.fullName")
      val existsPipelineInfoTable: Boolean = sparkSession.catalog.tableExists(pipelineInfoTableName, hiveDefaultDbName)
      val isPipelineInfoTableNotEmpty: Boolean = existsPipelineInfoTable && sparkSession.table(pipelineInfoTableNameFull).count > 0
      if (existsPipelineInfoTable & isPipelineInfoTableNotEmpty) {

        // If everything seems to be ok with pipelineInfoTable, look up for provided pipeline name
        logger.info(s"Table '$pipelineInfoTableNameFull' exists and it's not empty. " +
          s"Thus, looking for information on pipeline '$pipelineName' within it")

        val sqlQuery = s"SELECT file_name FROM $pipelineInfoTableNameFull WHERE trim(lower(pipeline_name)) = '${pipelineName.toLowerCase}'"
        val pipelineInfoRows: Array[Row] = sparkSession.sql(sqlQuery).collect()
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
        Some(jobProperties.getString("initialLoad.file.path"))
      }
    }
  }

  private def logToJDBC(logRecords: Seq[LogRecord], sparkSession: SparkSession, jobProperties: PropertiesConfiguration): Unit = {

    import sparkSession.implicits._

    val logRecordDf: DataFrame = logRecords.toDF
    val regex: scala.util.matching.Regex = "([A-Z])".r
    val logRecordDfWithTableColumnNameConvention: DataFrame = logRecordDf.columns
      .foldLeft(logRecordDf)((df, columnName) => {

        val newColumnName: String = regex.replaceAllIn(columnName, m => s"_${m.group(1).toLowerCase}")
        df.withColumnRenamed(columnName, newColumnName)
      })

    logger.info(s"Successfully turned list of ${logRecords.size} object(s) " +
      s"of type ${classOf[LogRecord].getSimpleName} " +
      s"into a ${classOf[DataFrame].getSimpleName}")

    // Extract relevant info for creating JDBC connection
    val jdbcLoggingDatabase = jobProperties.getString("jdbc.database.default.name")
    val jdbcUrl = jobProperties.getString("jdbc.default.url")
    val jdbcDriver = jobProperties.getString("jdbc.default.driver.className")
    val jdbcUserName = jobProperties.getString("jdbc.default.userName")
    val jdbcPassWord = jobProperties.getString("jdbc.default.passWord")
    val jdbcUseSSL = jobProperties.getString("jdbc.default.useSSL")

    // Create database hosting logTable if necessary
    val connection: java.sql.Connection = JDBCUtils.getConnection(jdbcUrl, jdbcDriver, jdbcUserName, jdbcPassWord, jdbcUseSSL)
    JDBCUtils.createDbIfNotExists(jdbcLoggingDatabase, connection)

    // Setup DataFrameWriter JDBC options
    val logTableFullName = jobProperties.getString("jdbc.table.logging.table.fullName")
    val sparkWriterJDBCOptions: Map[String, String] = JDBCUtils.getSparkWriterJDBCOptions(jdbcUrl, jdbcDriver, jdbcUserName, jdbcPassWord, jdbcUseSSL)
    logger.info(s"Logging dataframe schema: ${SparkUtils.dataframeSchema(logRecordDf)}")
    logRecordDfWithTableColumnNameConvention.coalesce(1)
      .write
      .format("jdbc")
      .options(sparkWriterJDBCOptions)
      .option("dbTable", logTableFullName)
      .mode(SaveMode.Append)
      .save

    logger.info(s"Successfully inserted ${logRecords.size} logging record(s) within table '$logTableFullName'")
  }

  def run(pipelineName: String, propertiesFile: String): Unit = {

    lazy val sparkSession: SparkSession = SparkUtils.getOrCreateSparkSession
    val jobProperties = new PropertiesConfiguration(propertiesFile)
    logger.info("Successfully loaded job .properties file")

    val pipelineFilePathOpt: Option[String] = getPipelineFilePathOpt(pipelineName, sparkSession, jobProperties)
    if (pipelineFilePathOpt.nonEmpty) {

      // Try to parse provided json file as a Pipeline and run it
      val pipeline: Pipeline = JsonUtils.decodeAndInterpolateJsonFile[Pipeline](pipelineFilePathOpt.get, jobProperties)
      val (pipelineFullyExecuted, logRecords) : (Boolean, Seq[LogRecord]) = pipeline.run(sparkSession)
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
