package it.luca.pipeline

import argonaut._
import it.luca.pipeline.data.LogRecord
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.utils.JobProperties
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

case class Pipeline(name: String, description: String, steps: Option[List[AbstractStep]]) {

  private final val logger = Logger.getLogger(classOf[LogRecord])

  def run(sparkSession: SparkSession, jobProperties: JobProperties): (Boolean, Seq[LogRecord]) = {

    (true, Seq.empty[LogRecord])
  }
}

object Pipeline {

  implicit def PipelineDecodeJson: DecodeJson[Pipeline] = DecodeJson.derive[Pipeline]
}