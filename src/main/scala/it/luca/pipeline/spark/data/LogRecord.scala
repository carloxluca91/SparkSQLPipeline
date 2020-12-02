package it.luca.pipeline.spark.data

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

    LogRecord(applicationId = sparkContext.applicationId,
      applicationName = sparkContext.appName,
      applicationStartTime = Timestamp.from(Instant.ofEpochMilli(sparkContext.startTime)),
      applicationStartDate = new Date(sparkContext.startTime),
      pipelineName = pipelineName,
      pipelineDescription = pipelineDescription,
      stepIndex = stepIndex,
      stepName = abstractStep.name,
      stepType = abstractStep.stepType,
      stepDescription = abstractStep.description,
      dataframeId = abstractStep.dataframeId,
      stepFinishTime = new Timestamp(System.currentTimeMillis()),
      stepFinishDate = new Date(System.currentTimeMillis()),
      stepFinishCode = if (exceptionOpt.isEmpty) 0 else -1,
      stepFinishStatus = if (exceptionOpt.isEmpty) "OK" else "KO",
      exceptionMessage = exceptionMsgOpt)
  }
}