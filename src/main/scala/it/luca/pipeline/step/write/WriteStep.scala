package it.luca.pipeline.step.write

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import org.apache.spark.sql.{DataFrame, SparkSession}

case class WriteStep(override val name: String,
                     override val description: String,
                     override val stepType: String,
                     override val dataframeId: String)
  extends AbstractStep(name, description, stepType, dataframeId) {

  def write(dataFrame: DataFrame, sparkSession: SparkSession): Unit = {
  }
}

object WriteStep {

  implicit def decodeJson: DecodeJson[WriteStep] = DecodeJson.derive[WriteStep]
}
