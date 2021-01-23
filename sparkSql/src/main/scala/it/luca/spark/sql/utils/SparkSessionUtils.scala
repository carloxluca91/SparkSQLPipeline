package it.luca.spark.sql.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SparkSessionUtils {

  private val log = Logger.getLogger(getClass)
  private val logSparkSessionInfo: (SparkSession, Boolean) => Unit =
    (sparkSession, withHiveSupport) => {

      val withHiveSupportInfo = if (withHiveSupport) "with Hive support" else "without Hive support"
      val sc = sparkSession.sparkContext
      val details =
        s"""Successfully initialized ${classOf[SparkSession].getSimpleName} $withHiveSupportInfo. Some details:
           |
           |     applicationName = '${sc.appName}',
           |     applicationId = ${sc.applicationId},
           |     sparkUser = ${sc.sparkUser},
           |     UI URL = ${sc.uiWebUrl.getOrElse("NOT AVAILABLE")}
           |     """.stripMargin

      log.info(details)
  }

  def getOrCreate: SparkSession = {

    log.info(s"Trying to initialize a standard ${classOf[SparkSession].getSimpleName} (i.e. without Hive support)")
    val sparkSession = SparkSession
      .builder
      .getOrCreate
    logSparkSessionInfo(sparkSession, false)
    sparkSession
  }

  def getOrCreateWithHiveSupport: SparkSession = {

    log.info(s"Trying to initialize a ${classOf[SparkSession].getSimpleName} with Hive support")
    val sparkSession = SparkSession.builder
      .enableHiveSupport
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate
    logSparkSessionInfo(sparkSession, true)
    sparkSession
  }
}
