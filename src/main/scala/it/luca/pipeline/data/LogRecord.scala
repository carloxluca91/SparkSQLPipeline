package it.luca.pipeline.data

import it.luca.pipeline.step.common.AbstractStep
import org.apache.spark.SparkContext

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}

case class LogRecord(applicationId: String,
                     applicationName: String,
                     applicationStartTime: Timestamp,
                     applicationStartDate: String,
                     applicationUser: String,
                     pipelineName: String,
                     pipelineDescription: String,
                     stepProgression: String,
                     stepName: String,
                     stepType: String,
                     stepDescription: String,
                     dataframeId: String,
                     stepFinishTime: Timestamp,
                     stepFinishDate: String,
                     stepFinishCode: Int,
                     stepFinishStatus: String,
                     exceptionMessage: Option[String],
                     yarnUIUrl: String)

object LogRecord {

  def apply(pipelineName: String,
            pipelineDescription: String,
            stepProgression: String,
            abstractStep: AbstractStep,
            sparkContext: SparkContext,
            exceptionOpt: Option[Throwable],
            yarnUIUrl: String): LogRecord = {

    LogRecord(applicationId = sparkContext.applicationId,
      applicationName = sparkContext.appName,
      applicationStartTime = new Timestamp(sparkContext.startTime),
      applicationStartDate = LocalDate.from(Instant.ofEpochMilli(sparkContext.startTime)).format(DateTimeFormatter.ISO_LOCAL_DATE),
      applicationUser = sparkContext.sparkUser,
      pipelineName = pipelineName,
      pipelineDescription = pipelineDescription,
      stepProgression = stepProgression,
      stepName = abstractStep.name,
      stepType = abstractStep.stepType,
      stepDescription = abstractStep.description,
      dataframeId = abstractStep.alias,
      stepFinishTime = new Timestamp(System.currentTimeMillis()),
      stepFinishDate = LocalDate.from(Instant.ofEpochMilli(System.currentTimeMillis())).format(DateTimeFormatter.ISO_LOCAL_DATE),
      stepFinishCode = if (exceptionOpt.isEmpty) 0 else -1,
      stepFinishStatus = if (exceptionOpt.isEmpty) "OK" else "KO",
      exceptionMessage = exceptionOpt.map(_.toString),
      yarnUIUrl = s"$yarnUIUrl/app/${sparkContext.applicationId}"
    )
  }
}