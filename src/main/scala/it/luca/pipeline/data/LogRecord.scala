package it.luca.pipeline.data

import java.sql.{Date, Timestamp}
import java.time.Instant

import it.luca.pipeline.step.common.AbstractStep
import org.apache.spark.SparkContext

case class LogRecord(applicationId: String,
                     applicationName: String,
                     applicationStartTime: Timestamp,
                     applicationStartDate: Date,
                     pipelineName: String,
                     pipelineDescription: String,
                     stepIndex: Int,
                     stepName: String,
                     stepType: String,
                     stepDescription: String,
                     dataframeId: String,
                     stepFinishTime: Timestamp,
                     stepFinishDate: Date,
                     stepFinishCode: Int,
                     stepFinishStatus: String,
                     exceptionMessage: Option[String])

object LogRecord {

  def apply(pipelineName: String,
            pipelineDescription: String,
            stepIndex: Int,
            abstractStep: AbstractStep,
            sparkContext: SparkContext,
            exceptionOpt: Option[Throwable]): LogRecord = {

    val applicationId: String = sparkContext.applicationId
    val applicationName: String = sparkContext.appName
    val applicationStartTime = Timestamp.from(Instant.ofEpochMilli(sparkContext.startTime))
    val applicationStartDate = new Date(sparkContext.startTime)
    val stepFinishTime = new Timestamp(System.currentTimeMillis())
    val stepFinishDate = new Date(System.currentTimeMillis())
    val exceptionMsgOpt: Option[String] = exceptionOpt match {
      case None => None
      case Some(e) =>

        val stackTraceStr: String = e.getStackTrace
          .take(10)
          .map(_.toString)
          .mkString("\n")

        val exceptionMsg: String = s"${e.toString}. Stack trace: $stackTraceStr"
        Some(exceptionMsg)
    }

    LogRecord(applicationId,
      applicationName,
      applicationStartTime,
      applicationStartDate,
      pipelineName,
      pipelineDescription,
      stepIndex,
      abstractStep.name,
      abstractStep.stepType,
      abstractStep.description,
      abstractStep.dataframeId,
      stepFinishTime,
      stepFinishDate,
      if (exceptionOpt.isEmpty) 0 else -1,
      if (exceptionOpt.isEmpty) "OK" else "KO",
      exceptionMsgOpt)
  }
}