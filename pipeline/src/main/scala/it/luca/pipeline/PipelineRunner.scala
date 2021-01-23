package it.luca.pipeline

import it.luca.pipeline.data.LogRecord
import it.luca.pipeline.exception.DuplicatePipelineException
import it.luca.pipeline.json.JsonUtils
import it.luca.pipeline.option.ScoptParser.InputConfiguration
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.utils.JobProperties
import it.luca.spark.sql.extensions._
import it.luca.spark.sql.utils.SparkSessionUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.io.Source

case class PipelineRunner(private val inputConfiguration: InputConfiguration) {

  private val log = Logger.getLogger(getClass)
  private val pipelineName: String = inputConfiguration.pipelineName

  private def getPipelineFilePathOpt(sparkSession: SparkSession, jobProperties: JobProperties): Option[String] = {

    val dbName = jobProperties.getString("hive.db.pipelineRunner.name").toLowerCase
    val tableName = jobProperties.getString("hive.table.pipelineInfo.name").toLowerCase

    val existsDb = sparkSession.catalog.databaseExists(dbName)
    val existsTable = if (existsDb) sparkSession.catalog.tableExists(dbName, tableName) else false
    val tableIsNotEmpty = if (existsTable) !sparkSession.table(s"$dbName.$tableName").isEmpty else false

    // If everything is ok, try to run provided pipeline
    if (existsDb & existsTable & tableIsNotEmpty) {

      log.info(s"Table '$dbName.$tableName' exists and it's not empty. Thus, looking for information on pipeline '$pipelineName' within it")
      val sqlQuery = s"""
           |
           |      SELECT pipeline_file_path
           |      FROM $dbName.$tableName
           |      WHERE pipeline_name = '$pipelineName'
           |      """.stripMargin

      val pipelineInfoRows: Array[Row] = sparkSession.sql(sqlQuery).collect()
      if (pipelineInfoRows.isEmpty) {
        log.warn(s"Unable to retrieve any info about pipeline '$pipelineName' with such query $sqlQuery. Thus, nothing will be triggered :(")
        None
      } else {
        if (pipelineInfoRows.length > 1) {
          throw DuplicatePipelineException(pipelineName, pipelineInfoRows)
        } else {
          Some(pipelineInfoRows(0).getAs(0))
        }
      }
    } else {

      // Otherwise, run 'INITIAL_LOAD' pipeline, independently on provided pipelineName
      val warningMsgPrefix: String = if (!existsDb) {
        s"Hive db '$dbName' does not exist"
      } else if (!existsTable) {
        s"Hive db '$dbName' exists but table '$tableName' does not"
      } else {
        s"Both Hive db '$dbName' and table '$tableName' exists but this latter is empty"
      }

      val warningMsgSuffix = if (pipelineName.equalsIgnoreCase("initial_load")) {
        ""
      } else {
        s"Thus, running pipeline 'INITIAL_LOAD' instead of provided one ('$pipelineName')"
      }

      log.warn(s"$warningMsgPrefix. $warningMsgSuffix")
      Some(jobProperties.getString("hdfs.pipeline.initialLoad.file.path"))
    }
  }

  private def insertLogRecords(logRecords: Seq[LogRecord], sparkSession: SparkSession, jobProperties: JobProperties): Unit = {

    import sparkSession.implicits._

    // Cast to DataFrame and rename column according to SQL convention
    val logRecordDf: DataFrame = logRecords.toDF
    val regex: scala.util.matching.Regex = "([A-Z])".r
    val logRecordDfWithSQLColumnNames: DataFrame = logRecordDf.columns
      .foldLeft(logRecordDf)((df, columnName) => {
        val newColumnName: String = regex.replaceAllIn(columnName, m => s"_${m.group(1).toLowerCase}")
        df.withColumnRenamed(columnName, newColumnName)
      })

    log.info(s"Successfully turned ${logRecords.size} ${classOf[LogRecord].getSimpleName}(s) " +
      s"into a ${classOf[DataFrame].getSimpleName}. Schema: ${logRecordDfWithSQLColumnNames.prettySchema}")

    // Define logging database if it does not exist
    val logTableDbName = jobProperties.getString("hive.db.pipelineRunner.name").toLowerCase
    val logTableName = jobProperties.getString("hive.table.pipelineLog.name").toLowerCase
    sparkSession.createDbIfNotExists(logTableDbName, None)

    // Save logging records
    val logTableFullName = s"$logTableDbName.$logTableName"
    val saveMode = if (sparkSession.catalog.tableExists(logTableDbName, logTableName)) "append" else "error"
    logRecordDfWithSQLColumnNames
      .coalesce(1)
      .saveAsTableOrInsertInto(logTableFullName,
        saveMode,
        partitionByOpt = None,
        tablePathOpt = None)

    log.info(s"Successfully inserted ${logRecords.size} ${classOf[LogRecord].getSimpleName}(s) within table '$logTableFullName'")
  }

  def run(): Unit = {

    val jobProperties = JobProperties(inputConfiguration.jobPropertiesFile)
    val sparkSession: SparkSession = SparkSessionUtils.getOrCreateWithHiveSupport

    // Retrieve HDFS path of .json file representing provided pipeline
    val pipelineFilePathOpt: Option[String] = getPipelineFilePathOpt(sparkSession, jobProperties)
    if (pipelineFilePathOpt.nonEmpty) {

      // Turn HDFS .json file into a json string
      val pipelineFilePath = pipelineFilePathOpt.get
      log.info(s"Content of pipeline '$pipelineName' available at HDFS path '$pipelineFilePath'")
      val hadoopFs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      val jsonString: String = Source
        .fromInputStream(hadoopFs.open(new Path(pipelineFilePath)))
        .getLines().mkString(" ")

      // Decode the json string as a Pipeline object and run it
      val decodedPipeline: Pipeline = JsonUtils.decodeAndInterpolateJsonString[Pipeline](jsonString, jobProperties)
      val logRecordPartialApply: (String, AbstractStep, Option[Throwable]) => LogRecord = LogRecord(decodedPipeline.name,
        decodedPipeline.description,
        _: String,
        _: AbstractStep,
        sparkSession.sparkContext,
        _: Option[Throwable],
        jobProperties.getString("yarn.application.history.ui.url"))

      val (pipelineFullyExecuted, logRecords) : (Boolean, Seq[LogRecord]) = decodedPipeline.run(sparkSession, logRecordPartialApply)

      // Insert LogRecords returned by pipeline (for logging purposes)
      insertLogRecords(logRecords, sparkSession, jobProperties)
      if (pipelineFullyExecuted) {
        log.info(s"Successfully executed whole pipeline '$pipelineName'")
      } else {
        log.warn(s"Unable to fully execute pipeline '$pipelineName'")
      }
    }
  }
}
