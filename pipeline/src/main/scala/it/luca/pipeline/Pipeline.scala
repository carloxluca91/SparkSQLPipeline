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

  private val logger = Logger.getLogger(classOf[Pipeline])
  private val dataframeMap: mutable.Map[String, DataFrame] = mutable.Map.empty[String, DataFrame]

  private def updateDataframeMap(dataFrameId:String, dataFrame: DataFrame): Unit = {

    val inputDfSchema: String = dataFrame.prettySchema
    if (dataframeMap contains dataFrameId) {

      // If dataFrameId is already defined, notify overwriting operation
      val oldDfSchema: String = dataframeMap(dataFrameId).prettySchema
      logger.warn(s"Dataframe id '$dataFrameId' is already defined. Schema: $oldDfSchema")
      logger.warn(s"It will be overwritten by a Dataframe having schema $inputDfSchema")

    } else {
      logger.info(s"Defining new entry '$dataFrameId' having schema $inputDfSchema")
    }

    dataframeMap(dataFrameId) = dataFrame
    logger.info(s"Successfully updated dataframeMap")
  }

  def run(sparkSession: SparkSession): (Boolean, Seq[LogRecord]) = {

    // If some steps are defined
    val logRecords: mutable.ListBuffer[LogRecord] = mutable.ListBuffer.empty[LogRecord]
    val numberOfSteps = steps.length
    for ((abstractStep, stepIndex) <- steps.zip(1 to numberOfSteps)) {

      // Try to execute them one by one according to matched pattern
      val stepName: String = abstractStep.name
      val stepProgression = s"$stepIndex/$numberOfSteps"
      logger.info(s"Starting to execute step # $stepIndex ('$stepName')")
      val tryToExecuteStep: Try[Unit] = Try {
        abstractStep match {
          case readStep: ReadStep =>
            val readDataframe: DataFrame = readStep.read(sparkSession)
            updateDataframeMap(readStep.outputAlias, readDataframe)

          case transformStep: TransformStep =>
            val transformedDataframe: DataFrame = transformStep.transform(dataframeMap)
            updateDataframeMap(transformStep.inputAlias, transformedDataframe)

          case writeStep: WriteStep =>
            val dataframeToWrite: DataFrame = dataframeMap(writeStep.inputAlias)
            writeStep.write(dataframeToWrite)
        }
      }

      tryToExecuteStep match {
        case Failure(e) =>

          // If the step triggered an exception, create a new LogRecord reporting what happened and return the ones gathered so far
          logger.error(s"Caught exception while trying to execute step # $stepIndex ('$stepName'). Stack trace: ", e)
          logRecords.append(LogRecord(name, description, stepProgression, abstractStep, sparkSession.sparkContext, Some(e)))
          return (false, logRecords)

        case Success(_) =>

          // Otherwise, just add this one and continue looping
          logger.info(s"Successfully executed step # $stepIndex ('$stepName')")
          logRecords.append(LogRecord(name, description, stepProgression, abstractStep, sparkSession.sparkContext, None))
      }
    }

    // If the loop has been fully executed, return true
    (true, logRecords)
  }
}

object Pipeline {

  implicit def decodeJson: DecodeJson[Pipeline] = DecodeJson.derive[Pipeline]
}