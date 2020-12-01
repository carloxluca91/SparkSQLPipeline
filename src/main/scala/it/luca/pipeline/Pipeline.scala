package it.luca.pipeline

import argonaut.DecodeJson
import it.luca.pipeline.exception.EmptyPipelineException
import it.luca.pipeline.spark.data.LogRecord
import it.luca.pipeline.spark.SparkUtils
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.read.ReadStep
import it.luca.pipeline.step.transform.TransformStep
import it.luca.pipeline.step.write.WriteStep
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class Pipeline(name: String, description: String, pipelineSteps: Option[List[AbstractStep]]) {

  private final val logger = Logger.getLogger(classOf[Pipeline])
  private final val dataframeMap: mutable.Map[String, DataFrame] = mutable.Map.empty[String, DataFrame]

  private def updateDataframeMap(dataframeId:String, dataFrame: DataFrame): Unit = {

    val inputDfSchema: String = SparkUtils.dataframeSchema(dataFrame)
    if (dataframeMap contains dataframeId) {

      val oldDfSchema: String = SparkUtils.dataframeSchema(dataframeMap(dataframeId))
      logger.warn(s"Dataframe id '$dataframeId' is already defined. Schema: $oldDfSchema")
      logger.warn(s"It will be overwritten by a Dataframe having schema $inputDfSchema")
    } else {

      logger.info(s"Defining new entry '$dataframeId' having schema $inputDfSchema")
    }

    dataframeMap(dataframeId) = dataFrame
    logger.info(s"Successfully updated dataframeMap")
  }

  def run(sparkSession: SparkSession): (Boolean, Seq[LogRecord]) = {

    if (pipelineSteps.nonEmpty) {

      // If some steps are defined
      val logRecords: mutable.ListBuffer[LogRecord] = mutable.ListBuffer.empty[LogRecord]
      for ((abstractStep, stepIndex) <- pipelineSteps.get.zipWithIndex) {

        // Try to execute them one by one according to matched pattern
        val stepName: String = abstractStep.name
        logger.info(s"Starting to execute step # $stepIndex ('$stepName')")
        val tryToExecuteStep: Try[Unit] = Try {
          abstractStep match {
            case readStep: ReadStep =>
              val readDataframe: DataFrame = readStep.read(sparkSession)
              updateDataframeMap(readStep.dataframeId, readDataframe)

            case transformStep: TransformStep =>
              val transformedDataframe: DataFrame = transformStep.transform(dataframeMap)
              updateDataframeMap(transformStep.dataframeId, transformedDataframe)

            case writeStep: WriteStep =>
              val dataframeToWrite: DataFrame = dataframeMap(writeStep.dataframeId)
              writeStep.write(dataframeToWrite)
          }
        }

        tryToExecuteStep match {
          case Failure(e) =>

            // If the step triggered an exception, create a new LogRecord reporting what happened and return the ones gathered so far
            logger.error(s"Caught exception while trying to execute step # $stepIndex ('$stepName'). Stack trace: ", e)
            logRecords.append(LogRecord(name, description, stepIndex, abstractStep, sparkSession.sparkContext, Some(e)))
            return (false, logRecords)

          case Success(_) =>

            // Otherwise, just add this one and continue looping
            logger.info(s"Successfully executed step # $stepIndex ('$stepName')")
            logRecords.append(LogRecord(name, description, stepIndex, abstractStep, sparkSession.sparkContext, None))
        }
      }

      // If the loop has been fully executed, return true
      (true, logRecords)

    } else throw EmptyPipelineException(name)
  }
}

object Pipeline {

  implicit def decodeJson: DecodeJson[Pipeline] = DecodeJson.derive[Pipeline]
}