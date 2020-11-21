package it.luca.pipeline.step.read

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.utils.JobProperties
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ReadStep(override val name: String,
                    override val description: String,
                    override val stepType: String,
                    override val dataframeId: String)
                    //srcOptions: SrcOptions)
  extends AbstractStep(name, description, stepType, dataframeId) {

  def read(sparkSession: SparkSession, jobProperties: JobProperties): DataFrame = {
    sparkSession.emptyDataFrame
  }
}

object ReadStep {

  implicit def ReadStepDecodeJson: DecodeJson[ReadStep] = DecodeJson.derive[ReadStep]
}
