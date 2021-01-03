package it.luca.pipeline

import argonaut.DecodeJson
import it.luca.pipeline.data.LogRecord
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.read.ReadStep
import it.luca.pipeline.step.transform.TransformStep
import it.luca.pipeline.step.write.WriteStep
import it.luca.spark.sql.extensions._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class Pipeline(name: String, description: String, steps: List[AbstractStep]) {

  private val log = Logger.getLogger(classOf[Pipeline])
  private val dataframeMap: mutable.Map[String, DataFrame] = mutable.Map.empty[String, DataFrame]

  private def updateDataframeMap(dataFrameId:String, dataFrame: DataFrame): Unit = {

    val inputDfSchema: String = dataFrame.prettySchema
    if (dataframeMap contains dataFrameId) {

      // If dataFrameId is already defined, notify overwriting operation
      val oldDfSchema: String = dataframeMap(dataFrameId).prettySchema
      log.warn(s"Dataframe id '$dataFrameId' is already defined. Schema: $oldDfSchema")
      log.warn(s"It will be overwritten by a Dataframe having schema $inputDfSchema")

    } else {
      log.info(s"Defining new entry '$dataFrameId' having schema $inputDfSchema")
    }

    dataframeMap(dataFrameId) = dataFrame
    log.info(s"Successfully updated dataframeMap")
  }

  def run(sparkSession: SparkSession, toLogRecord: (String, AbstractStep, Option[Throwable]) => LogRecord): (Boolean, Seq[LogRecord]) = {

    val logRecords: mutable.ListBuffer[LogRecord] = mutable.ListBuffer.empty[LogRecord]
    val numberOfSteps = steps.length

    // For each step
    for ((abstractStep, stepIndex) <- steps.zip(1 to numberOfSteps)) {

      val stepName: String = abstractStep.name
      val stepProgression = s"$stepIndex/$numberOfSteps"

      // Try to execute it according to its matched pattern
      log.info(s"Starting to execute step # $stepIndex ('$stepName')")
      val tryToExecuteStep: Try[Unit] = Try {
        abstractStep match {
          case readStep: ReadStep =>
            val readDataframe: DataFrame = readStep.read(sparkSession)
            updateDataframeMap(readStep.outputAlias, readDataframe)

          case transformStep: TransformStep =>
            val transformedDataframe: DataFrame = transformStep.transform(dataframeMap)
            updateDataframeMap(transformStep.outputAlias, transformedDataframe)

          case writeStep: WriteStep =>
            val dataframeToWrite: DataFrame = dataframeMap(writeStep.inputAlias)
            writeStep.write(dataframeToWrite)
        }
      }

      tryToExecuteStep match {
        case Failure(e) =>

          // If the step triggered an exception, create a new LogRecord reporting what happened and return the ones gathered so far
          log.error(s"Caught exception while trying to execute step # $stepIndex ('$stepName'). Stack trace: ", e)
          logRecords.append(toLogRecord(stepProgression, abstractStep, Some(e)))
          return (false, logRecords)

        case Success(_) =>

          // Otherwise, just add this one and continue looping
          log.info(s"Successfully executed step # $stepIndex ('$stepName')")
          logRecords.append(toLogRecord(stepProgression, abstractStep, None))
      }
    }

    // If the loop has been fully executed, return true
    (true, logRecords)
  }
}

object Pipeline {

  implicit def decodeJson: DecodeJson[Pipeline] = DecodeJson.derive[Pipeline]
}