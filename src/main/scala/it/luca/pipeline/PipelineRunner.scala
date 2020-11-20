package it.luca.pipeline

import it.luca.pipeline.utils.JobProperties
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object PipelineRunner {

  private final val logger = Logger.getLogger(getClass)

  private def getSparkSession: SparkSession = {

    logger.info(s"Trying to initialize a ${classOf[SparkSession].getSimpleName}")

    val sparkSession = SparkSession.builder
      .enableHiveSupport
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate

    logger.info(s"Successfully initialized ${classOf[SparkSession].getSimpleName} " +
      s"for application '${sparkSession.sparkContext.appName}', " +
      s"applicationId = ${sparkSession.sparkContext.applicationId}, " +
      s"UI url = ${sparkSession.sparkContext.uiWebUrl}")

    sparkSession
  }

  def run(pipelineName: String, propertiesFile: String): Unit = {

    val sparkSession = getSparkSession
    val jobProperties = JobProperties(propertiesFile)
  }
}
