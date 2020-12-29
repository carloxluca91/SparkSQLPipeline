package it.luca.pipeline

import it.luca.pipeline.data.LogRecord
import it.luca.pipeline.json.JsonUtils
import it.luca.pipeline.option.ScoptParser.InputConfiguration
import it.luca.spark.sql.SparkSessionUtils
import it.luca.spark.sql.extensions._
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

case class PipelineRunner(private val inputConfiguration: InputConfiguration) {

  private val log = Logger.getLogger(getClass)
  private val pipelineName: String = inputConfiguration.pipelineName

  private def getPipelineFilePathOpt(sparkSession: SparkSession, jobProperties: PropertiesConfiguration): Option[String] = {

    val dbName = jobProperties.getString("hive.db.pipelineRunner.name").toLowerCase
    val tableName = jobProperties.getString("hive.table.pipelineInfo.name").toLowerCase

    // If everything is ok, run provided pipeline (or at least try to ;))
    val existsDb = sparkSession.catalog.databaseExists(dbName)
    val existsTable = if (existsDb) sparkSession.catalog.tableExists(dbName, tableName) else false
    val tableIsNotEmpty = if (existsTable) sparkSession.table(s"$dbName.$tableName").isEmpty else false
    if (existsDb & existsTable & tableIsNotEmpty) {

      log.info(s"Table '$dbName.$tableName' exists and it's not empty. Thus, looking for information on pipeline '$pipelineName' within it")
      val sqlQuery = s"""
           |
           |      SELECT file_name
           |      FROM $dbName.$tableName
           |      WHERE TRIM(LOWER(pipeline_name)) = '${pipelineName.toLowerCase}'
           |      """.stripMargin

      val pipelineInfoRows: Array[Row] = sparkSession.sql(sqlQuery).collect()
      if (pipelineInfoRows.isEmpty) {

        log.warn(s"Unable to retrieve any info about pipeline '$pipelineName' with such query $sqlQuery")
        None
      } else {

        val pipelineJsonFilePath: String = pipelineInfoRows(0).getAs(0)
        log.info(s"Json file for pipeline '$pipelineName': '$pipelineJsonFilePath'")
        Some(pipelineJsonFilePath)
      }
    } else {

      // Run 'INITIAL_LOAD' pipeline, independently on provided pipelineName
      val warningMsg: String = if (!existsDb) {
        s"Hive db '$dbName' does not exist"
      } else if (!existsTable) {
        s"Hive db '$dbName' exists but table '$tableName' does not"
      } else {
        s"Both Hive db '$dbName' and table '$tableName' exists but this latter is empty"
      }

      log.warn(warningMsg)
      Some(jobProperties.getString("hdfs.initialLoad.file.path"))
    }
  }

  private def insertLogRecords(logRecords: Seq[LogRecord], sparkSession: SparkSession, jobProperties: PropertiesConfiguration): Unit = {

    import sparkSession.implicits._

    // Cast to DataFrame and rename column according to SQL convention
    val logRecordDf: DataFrame = logRecords.toDF
    val regex: scala.util.matching.Regex = "([A-Z])".r
    val logRecordDfWithSQLColumnNames: DataFrame = logRecordDf.columns
      .foldLeft(logRecordDf)((df, columnName) => {

        val newColumnName: String = regex.replaceAllIn(columnName, m => s"_${m.group(1).toLowerCase}")
        df.withColumnRenamed(columnName, newColumnName)
      })
      .coalesce(1)

    log.info(s"Successfully turned list of ${logRecords.size} ${classOf[LogRecord].getSimpleName}(s) " +
      s"into a ${classOf[DataFrame].getSimpleName}. Schema: ${logRecordDfWithSQLColumnNames.prettySchema}")

    val logTableDbName = jobProperties.getString("hive.db.pipelineRunner.name").toLowerCase
    val logTableName = jobProperties.getString("hive.table.pipelineLog.name").toLowerCase
    val logTableFullName = s"$logTableDbName.$logTableName"

    // If logging table does not exist, use .saveAsTable
    sparkSession.createDbIfNotExists(logTableDbName, None)
    if (!sparkSession.catalog.tableExists(logTableDbName, logTableName)) {

      log.warn(s"Logging table '$logTableFullName' does not exists yet. Creating it now using .saveAsTable")
      logRecordDfWithSQLColumnNames
        .write
        .mode(SaveMode.ErrorIfExists)
        .saveAsTable(logTableFullName)

    } else {

      // Otherwise, just .insertInto
      log.info(s"Logging table '$logTableFullName' already exists. Starting to insert data within it using .insertInto")
      logRecordDfWithSQLColumnNames
        .write
        .mode(SaveMode.Append)
        .insertInto(logTableFullName)
    }

    log.info(s"Successfully inserted ${logRecords.size} ${classOf[LogRecord]}(s) within table '$logTableFullName'")
  }

  def run(): Unit = {

    val jobProperties = new PropertiesConfiguration(inputConfiguration.jobPropertiesFile)
    log.info("Successfully loaded job .properties file")

    lazy val sparkSession: SparkSession = SparkSessionUtils.getOrCreateWithHiveSupport
    val pipelineFilePathOpt: Option[String] = getPipelineFilePathOpt(sparkSession, jobProperties)
    if (pipelineFilePathOpt.nonEmpty) {

      // Try to parse provided json file as a Pipeline and run it
      val pipeline: Pipeline = JsonUtils.decodeAndInterpolateJsonFile[Pipeline](pipelineFilePathOpt.get, jobProperties)
      val (pipelineFullyExecuted, logRecords) : (Boolean, Seq[LogRecord]) = pipeline.run(sparkSession)
      insertLogRecords(logRecords, sparkSession, jobProperties)
      if (pipelineFullyExecuted) {
        log.info(s"Successfully executed whole pipeline '$pipelineName'")
      } else {
        log.warn(s"Unable to fully execute pipeline '$pipelineName'")
      }
    } else {
      log.warn(s"Unable to retrieve any record(s) related to pipeline '$pipelineName'. Thus, nothing will be triggered")
    }
  }
}
