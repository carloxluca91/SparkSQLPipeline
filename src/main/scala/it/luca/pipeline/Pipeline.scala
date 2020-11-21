package it.luca.pipeline

import argonaut.Argonaut.jdecode3L
import argonaut._
import it.luca.pipeline.data.LogRecord
import it.luca.pipeline.exception.EmptyPipelineException
import it.luca.pipeline.json.JsonField
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.read.ReadStep
import it.luca.pipeline.step.transform.TransformStep
import it.luca.pipeline.step.write.WriteStep
import it.luca.pipeline.utils.{JobProperties, Utils}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class Pipeline(name: String, description: String, pipelineStepsOpt: Option[List[AbstractStep]]) {

  private final val logger = Logger.getLogger(classOf[LogRecord])
  private final val dataframeMap: mutable.Map[String, DataFrame] = mutable.Map.empty[String, DataFrame]

  private def updateDataframeMap(dataframeId: String, dataFrame: DataFrame): Unit = {

    val dataFrameSchema: String = Utils.datasetSchema(dataFrame)
    if (dataframeMap contains dataframeId) {

      logger.warn(s"Dataframe id '$dataframeId' is already defined. Schema: ${Utils.datasetSchema(dataframeMap(dataframeId))}")
      logger.warn(s"It will be overwritten by a Dataframe having schema $dataFrameSchema")
    } else {

      logger.info(s"Defining new entry '$dataframeId' having schema $dataFrameSchema")
    }

    dataframeMap(dataframeId) = dataFrame
  }

  def run(sparkSession: SparkSession, jobProperties: JobProperties): (Boolean, Seq[LogRecord]) = {

    if (pipelineStepsOpt.nonEmpty) {

      // If some steps are defined
      val logRecords: mutable.ListBuffer[LogRecord] = mutable.ListBuffer.empty[LogRecord]
      for (abstractStep: AbstractStep <- pipelineStepsOpt.get) {

        // Try to execute them one by one according to matched pattern
        val (stepName, stepType): (String, String) = (abstractStep.name, abstractStep.stepType)
        val tryToExecuteStep: Try[Unit] = Try {
          abstractStep match {

            case readStep: ReadStep =>
              val readDataframe: DataFrame = readStep.read(sparkSession, jobProperties)
              updateDataframeMap(readStep.dataframeId, readDataframe)

            case transformStep: TransformStep =>
              val transformedDataframe: DataFrame = transformStep.transform(dataframeMap)
              updateDataframeMap(transformStep.dataframeId, transformedDataframe)

            case writeStep: WriteStep =>
              val dataframeToWrite: DataFrame = dataframeMap(writeStep.dataframeId)
              writeStep.write(dataframeToWrite, sparkSession)

            case _ =>
          }
        }

        tryToExecuteStep match {
          case Failure(e) =>

            // If the step triggered an exception, create a new LogRecord reporting what happened and return ones gathered so far
            logger.error(s"Caught exception while trying to execute step '$stepName' (type '$stepType'). Stack trace: ", e)
            logRecords.append(LogRecord(name, description, abstractStep, sparkSession.sparkContext, Some(e)))
            return (false, logRecords)

          case Success(_) =>

            // Otherwise, just add this one and continue looping
            logger.info(s"Successfully executed step '$stepName' (type '$stepType')")
            logRecords.append(LogRecord(name, description, abstractStep, sparkSession.sparkContext, None))
        }
      }

      // If the loop has been fully executed, return true
      (true, logRecords)

    } else throw EmptyPipelineException(name)
  }
}

object Pipeline {

  implicit def PipelineDecodeJson: DecodeJson[Pipeline] =
    jdecode3L(Pipeline.apply)(JsonField.Name.label, JsonField.Description.label, JsonField.PipelineSteps.label)
}